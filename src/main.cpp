/*
 * @file: main.cpp
 *
 *  Created on: Aug 21, 2018
 *      Author: Viacheslav Iesmanskyi <rino4work@gmail.com>
 */

#include <iostream>

#include "BlobSort.h"

int main(int argc, char* argv[])
{
	if (argc != 3)
	{
		std::cerr << "Usage: blobsort <in_file> <out_file>\n";
		return EXIT_FAILURE;
	}

	try
	{
		std::ios_base::sync_with_stdio(false);
		ring::SortBlob32(argv[1], argv[2]);
		std::cout << "Finished\n";
	}
	catch (const ring::SortException& e)
	{
		std::cout << e.what() << std::endl;
	}

	return EXIT_SUCCESS;
}
