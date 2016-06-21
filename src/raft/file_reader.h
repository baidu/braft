// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2016/06/16 17:05:38

#ifndef  PUBLIC_RAFT_FILE_READER_H
#define  PUBLIC_RAFT_FILE_READER_H

#include <set>                              // std::set
#include <base/memory/ref_counted.h>        // base::RefCountedThreadsafe
#include <base/iobuf.h>                     // base::IOBuf

namespace raft {

// Abstrace class to read data from a file
// All the const method should be thread safe
class FileReader : public base::RefCountedThreadSafe<FileReader> {
friend class base::RefCountedThreadSafe<FileReader>;
public:
    // Read data from filename at |offset| (from the start of the file) for at
    // most |max_count| bytes to |out|. set |is_eof| to true if reaches to the
    // end of the file.
    // Returns 0 on success, -1 otherwise
    virtual int read_file(base::IOBuf* out,
                          const std::string &filename,
                          off_t offset,
                          size_t max_count,
                          bool* is_eof) const = 0;
    // Get the path of this reader
    virtual const char* path() const = 0;
protected:
    FileReader() {}
    virtual ~FileReader() {}
};

// Read files within a local directory
class LocalDirReader : public FileReader {
public:
    LocalDirReader(const std::string& path) 
        : _path(path)
        , _has_white_list(false)
    {}
    virtual ~LocalDirReader() {}
    // Read data from filename at |offset| (from the start of the file) for at
    // most |max_count| bytes to |out|. set |is_eof| to true if reaches to the
    // end of the file.
    // Returns 0 on success, -1 otherwise
    virtual int read_file(base::IOBuf* out,
                          const std::string &filename,
                          off_t offset,
                          size_t max_count,
                          bool* is_eof) const;
    // Add file to the white list.
    // After this function is called , no files except the files in the white
    // list could be read from this Reader.
    void add_white_list(const std::string& filename);

    virtual const char* path() const { return _path.c_str(); }
private:
    std::string _path;
    bool _has_white_list;
    std::set<std::string> _white_list;
};

}  // namespace raft

#endif  //PUBLIC_RAFT_FILE_READER_H
