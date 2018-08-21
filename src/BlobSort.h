/*
 * @file: BlobSort.h
 *
 *  Created on: Aug 21, 2018
 *      Author: Viacheslav Iesmanskyi <rino4work@gmail.com>
 */

#pragma once

#include <string>
#include <stdexcept>

namespace ring
{

class SortException: public std::runtime_error
{
public:
	SortException(const std::string& msg)
		: std::runtime_error(msg)
	{
	}
};

/**
 * @brief Sort blob file
 *
 * Sort binary large object file.
 * The file size must be a multiple of 4Gb up to 16Gb.
 * The file is treated as a contigious array of 32-bit unsigned values.
 *
 * @param[in] inFilePath - input file path (a file to sort)
 * @param[in] outFilePath - output file path (a file to store sorted values)
 *
 * @throw @ref ring::SortException
 *
 * @return None
 */
void SortBlob32(const std::string& inFilePath, const std::string& outFilePath);

} /* namespace ring */
