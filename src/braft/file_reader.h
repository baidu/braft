// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Zhangyi Chen(chenzhangyi01@baidu.com)
//          Zheng,Pengfei(zhengpengfei@baidu.com)
//          Xiong,Kai(xiongkai@baidu.com)
//          Yang,Guodong(yangguodong01@baidu.com)

#ifndef  BRAFT_FILE_READER_H
#define  BRAFT_FILE_READER_H

#include <set>                              // std::set
#include <butil/memory/ref_counted.h>        // butil::RefCountedThreadsafe
#include <butil/iobuf.h>                     // butil::IOBuf
#include "braft/file_system_adaptor.h"

namespace braft {

// Abstract class to read data from a file
// All the const method should be thread safe
class FileReader : public butil::RefCountedThreadSafe<FileReader> {
friend class butil::RefCountedThreadSafe<FileReader>;
public:
    // Read data from filename at |offset| (from the start of the file) for at
    // most |max_count| bytes to |out|. Reading part of a file is allowed if 
    // |read_partly| is TRUE. If successfully read the file, |read_count|
    // is the actual read count, it's maybe smaller than |max_count| if the
    // request is throttled or reach the end of the file. In the case of
    // reaching the end of the file, |is_eof| is also set to true.
    // Returns 0 on success, the error otherwise
    virtual int read_file(butil::IOBuf* out,
                          const std::string &filename,
                          off_t offset,
                          size_t max_count,
                          bool read_partly,
                          size_t* read_count,
                          bool* is_eof) const = 0;
    // Get the path of this reader
    virtual const std::string& path() const = 0;
protected:
    FileReader() {}
    virtual ~FileReader() {}
};

// Read files within a local directory
class LocalDirReader : public FileReader {
public:
    LocalDirReader(FileSystemAdaptor* fs, const std::string& path) 
        : _path(path), _fs(fs)
    {}
    virtual ~LocalDirReader();

    // Open a snapshot for read
    virtual bool open();
    
    // Read data from filename at |offset| (from the start of the file) for at
    // most |max_count| bytes to |out|. Reading part of a file is allowed if 
    // |read_partly| is TRUE. If successfully read the file, |read_count|
    // is the actual read count, it's maybe smaller than |max_count| if the
    // request is throttled or reach the end of the file. In the case of
    // reaching the end of the file, |is_eof| is also set to true.
    // Returns 0 on success, the error otherwise
    virtual int read_file(butil::IOBuf* out,
                          const std::string &filename,
                          off_t offset,
                          size_t max_count,
                          bool read_partly,
                          size_t* read_count,
                          bool* is_eof) const;
    virtual const std::string& path() const { return _path; }
protected:
    int read_file_with_meta(butil::IOBuf* out,
                            const std::string &filename,
                            google::protobuf::Message* file_meta,
                            off_t offset,
                            size_t max_count,
                            size_t* read_count,
                            bool* is_eof) const;
    const scoped_refptr<FileSystemAdaptor>& file_system() const { return _fs; }

private:
    std::string _path;
    scoped_refptr<FileSystemAdaptor> _fs;
};

}  //  namespace braft

#endif  //BRAFT_FILE_READER_H
