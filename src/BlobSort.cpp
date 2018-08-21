/*
 * @file: BlobSort.cpp
 *
 *  Created on: Aug 21, 2018
 *      Author: Viacheslav Iesmanskyi <rino4work@gmail.com>
 */

#include "BlobSort.h"

#include <cstring>
#include <vector>
#include <queue>
#include <algorithm>
#include <iterator>
#include <thread>
#include <future>
#include <mutex>
#include <condition_variable>
#include <experimental/filesystem>
#include <fstream>
#include <sstream>
#include <iomanip>

namespace fs = std::experimental::filesystem;

namespace ring
{

namespace
{

constexpr auto MAX_ALLOCATED_MEMORY_SIZE = 256LL << 20; ///< 256Mb - Max allowed memory allocation size
constexpr auto FILE_SIZE_MULTIPLIER = 4; ///<  4byte - File size must be a multiple of this

/**
 * @brief uint32_t wrapper class
 *
 * Wrapper class to enable uint32_t streamed input/output.
 * Also provides 'operator<' for sorting it's values.
 */
class Uint32Value
{
	friend std::istream& operator>>(std::istream&, Uint32Value&);
	friend std::ostream& operator<<(std::ostream&, const Uint32Value&);

public:
	bool operator<(const Uint32Value& other) const
		{
		return m_value < other.m_value;
	}

private:
	uint32_t m_value = 0;
};

std::istream& operator>>(std::istream& strm, Uint32Value& value)
{
	return strm.read(reinterpret_cast<char*>(&value.m_value), sizeof(value.m_value));
}

std::ostream& operator<<(std::ostream& strm, const Uint32Value& value)
{
	return strm.write(reinterpret_cast<const char*>(&value.m_value), sizeof(value.m_value));
}

/**
 * @brief Simple blocking memory pool
 */
class SimpleBlockingMemoryPool
{
	friend class Chunk;

public:
	/**
	 * @brief Memory chunk
	 *
	 * A RAII helper class to automatically release acquired chunk
	 */
	class Chunk
	{
		friend class SimpleBlockingMemoryPool;

	public:
		Chunk(const Chunk&) = delete;
		Chunk& operator=(const Chunk&) = delete;

		Chunk(Chunk&& other)
			:
				m_pool(other.m_pool)
		{
			std::swap(m_buff, other.m_buff);
		}

		operator char*()
		{
			return m_buff;
		}

		char* Data()
		{
			return m_buff;
		}

		~Chunk() noexcept
		{
			m_pool.Release(m_buff);
		}

	private:
		explicit Chunk(SimpleBlockingMemoryPool& pool)
			: m_pool(pool)
		{
		}

		SimpleBlockingMemoryPool& m_pool;
		char* m_buff = nullptr;
	};

	SimpleBlockingMemoryPool(const SimpleBlockingMemoryPool&) = delete;
	SimpleBlockingMemoryPool operator=(const SimpleBlockingMemoryPool&) = delete;

	/**
	 * @biref Constructor
	 *
	 * Construct pool given object size and count
	 *
	 * @param size - size of object in bytes
	 * @param count - count of objects in pool.
	 */
	SimpleBlockingMemoryPool(int size, int count)
		: m_buff(size * count)
	{
		for (int i = 0; i < count; i++)
		{
			m_queue.push(&m_buff[i * size]);
		}
	}

	/**
	 * @brief Acquire chunk of memory
	 *
	 * Blocks if pool is empty
	 *
	 * @return A chunk of memory
	 */
	Chunk Acquire()
	{
		auto chunk = Chunk(*this);

		std::unique_lock<std::mutex> lock(m_mutex);

		m_queueCond.wait(lock, [=](){ return !m_queue.empty(); });

		chunk.m_buff = m_queue.front();
		m_queue.pop();

		return chunk;
	}

private:
	void Release(char* chunk)
	{
		{
			std::unique_lock<std::mutex> lock(m_mutex);
			m_queue.push(chunk);
		}

		m_queueCond.notify_one();
	}

	std::mutex m_mutex;
	std::vector<char> m_buff;
	std::condition_variable m_queueCond;
	std::queue<char*> m_queue;
};

/**
 * @brief Create temp directory
 *
 * Creates temp directory with unique name
 *
 * @param[in] pathTemplate - directory name template prefix. Must be in form "<prefix>_XXXXXX"
 *
 * @return Unique temp directory path
 */
fs::path CreateUniqueTempDirectory(const std::string& pathTemplate)
{
	// Need to make a copy as mkdtemp returns a pointer to the modified template string
	std::vector<char> buff(pathTemplate.size() + 1);

	strncpy(buff.data(), pathTemplate.data(), buff.size());

	if (!mkdtemp(buff.data()))
	{
		throw SortException("Failed to create temp directory");
	}

	return buff.data();
}

struct Blob32Sorter
{
	Blob32Sorter(const std::string& inFilePath, const std::string& outFilePath)
		: m_inFilePath(inFilePath)
			, m_outFilePath(outFilePath)
			, m_inFileSize(fs::file_size(inFilePath))
			, m_tempDirPath(CreateUniqueTempDirectory(fs::temp_directory_path() / "blobsort_XXXXXX"))
			, m_memoryChunkSize(CalcMemoryChunkSize())
			, m_memPool(m_memoryChunkSize, MAX_ALLOCATED_MEMORY_SIZE / m_memoryChunkSize)
	{
		if (m_inFileSize % FILE_SIZE_MULTIPLIER)
		{
			throw SortException("File size is not a multiple of 4Gb");
		}
	}

	~Blob32Sorter() noexcept
	{
		std::error_code ec;
		if (fs::exists(m_tempDirPath, ec))
		{
			fs::remove_all(m_tempDirPath, ec);
		}
	}

	static int CalcMemoryChunkSize()
	{
		// Two chunks per CPU core
		return MAX_ALLOCATED_MEMORY_SIZE / (std::thread::hardware_concurrency() * 2);
	}

	void ReadChunk(char* chunk, uintmax_t offset, uintmax_t size)
	{
		std::ifstream strm(m_inFilePath, std::ios::binary);

		if (!strm.seekg(offset, std::ios::beg).read(chunk, size))
		{
			throw SortException("Failed to read input chunk");
		}
	}

	auto CreateChunkFileName(uintmax_t offset, uintmax_t size)
	{
		std::stringstream strm;

		strm << std::hex << std::setfill('0')
			<< std::setw(8) << offset << ':'
			<< std::setw(8) << size;

		return m_tempDirPath / strm.str();
	}

	auto CreateSortedChunk(uintmax_t offset, uintmax_t size, const fs::path& fileName)
	{
		auto chunk = m_memPool.Acquire();
		ReadChunk(chunk, offset, size);

		auto begin = reinterpret_cast<uint32_t*>(chunk.Data());
		auto end = reinterpret_cast<uint32_t*>(chunk + size);

		std::sort(begin, end);

		auto chunkFileName = fileName.empty() ? CreateChunkFileName(offset, size) :
			fileName;

		std::ofstream strm(chunkFileName, std::ios::binary);

		if (!strm.write(chunk, size))
		{
			throw SortException("Failed to open file to write sorted chunk");
		}

		return chunkFileName;
	}

	void MergeChunks(const fs::path& left, const fs::path& right, const fs::path& result)
	{
		// Use printf here instead of std::cout to avoid interleaving messages from different threads
		printf("Merging %s and %s into %s\n", left.c_str(), right.c_str(), result.c_str());

		std::ifstream leftStrm(left, std::ios::binary);
		std::ifstream rightStrm(right, std::ios::binary);
		std::ofstream resultStrm(result, std::ios::binary);
		std::istream_iterator<Uint32Value> eos;

		std::merge(std::istream_iterator<Uint32Value>(leftStrm), eos,
			std::istream_iterator<Uint32Value>(rightStrm), eos,
			std::ostream_iterator<Uint32Value>(resultStrm));

		if (leftStrm.bad() || rightStrm.bad() || resultStrm.bad())
		{
			throw SortException("I/O error during chunks merge");
		}
	}

	fs::path MapReduceChunks(uintmax_t offset, uintmax_t size, const fs::path& fileName)
	{
		auto halfSize = size / 2;
		auto leftOffset = offset;
		auto rightOffset = offset + halfSize;

		auto leftBranch = std::async(std::launch::async, [=]()
		{	return MapReduceOrCreateSortedChunks(leftOffset, halfSize);});
		auto rightBranch = std::async(std::launch::async, [=]()
		{	return MapReduceOrCreateSortedChunks(rightOffset, halfSize);});

		leftBranch.wait();
		rightBranch.wait();

		auto leftChunkFileName = leftBranch.get();
		auto rightChunkFileName = rightBranch.get();
		auto mergedFileName = (size < m_inFileSize) ? CreateChunkFileName(offset, size) : m_outFilePath;

		MergeChunks(leftChunkFileName, rightChunkFileName, mergedFileName);

		std::error_code ec; // Suppress exception on error
		fs::remove(leftChunkFileName, ec);
		fs::remove(rightChunkFileName, ec);

		return mergedFileName;
	}

	fs::path MapReduceOrCreateSortedChunks(uintmax_t offset, uintmax_t size, const fs::path& fileName = fs::path())
	{
		return (size > m_memoryChunkSize) ? MapReduceChunks(offset, size, fileName) :
			CreateSortedChunk(offset, size, fileName);
	}

	void Sort()
	{
		MapReduceOrCreateSortedChunks(0, m_inFileSize, m_outFilePath);
	}

	const fs::path m_inFilePath;
	const fs::path m_outFilePath;
	const uintmax_t m_inFileSize;

	fs::path m_tempDirPath;

	uintmax_t m_memoryChunkSize;
	SimpleBlockingMemoryPool m_memPool;
};

}

void SortBlob32(const std::string& inFilePath, const std::string& outFilePath)
{
	try
	{
		Blob32Sorter(inFilePath, outFilePath).Sort();
	}
	catch (const std::system_error& e)
	{
		throw SortException(e.what());
	}
}

} /* namespace ring */
