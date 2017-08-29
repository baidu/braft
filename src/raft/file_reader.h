// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2016/06/16 17:05:38

#ifndef  PUBLIC_RAFT_FILE_READER_H
#define  PUBLIC_RAFT_FILE_READER_H

#include <set>                              // std::set
#include <base/memory/ref_counted.h>        // base::RefCountedThreadsafe
#include <base/iobuf.h>                     // base::IOBuf
#include "raft/file_system_adaptor.h"

namespace raft {

// Abstract class to read data from a file
// All the const method should be thread safe
class FileReader : public base::RefCountedThreadSafe<FileReader> {
friend class base::RefCountedThreadSafe<FileReader>;
public:
    // Read data from filename at |offset| (from the start of the file) for at
    // most |max_count| bytes to |out|. Reading part of a file is allowed if 
    // |read_partly| is TRUE. set |is_eof| to true if reaching to the end of the 
    // file. 
    // Returns 0 on success, -1 otherwise
    virtual int read_file(base::IOBuf* out,
                          const std::string &filename,
                          off_t offset,
                          size_t max_count,
                          bool read_partly,
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
    // |read_partly| is TRUE. set |is_eof| to true if reaching to the end of the 
    // file. 
    // Returns 0 on success, -1 otherwise
    virtual int read_file(base::IOBuf* out,
                          const std::string &filename,
                          off_t offset,
                          size_t max_count,
                          bool read_partly,
                          bool* is_eof) const;
    virtual const std::string& path() const { return _path; }
protected:
    int read_file_with_meta(base::IOBuf* out,
                            const std::string &filename,
                            google::protobuf::Message* file_meta,
                            off_t offset,
                            size_t max_count,
                            bool* is_eof) const;
    const scoped_refptr<FileSystemAdaptor>& file_system() const { return _fs; }

private:
    std::string _path;
    scoped_refptr<FileSystemAdaptor> _fs;
};

}  // namespace raft

#endif  //PUBLIC_RAFT_FILE_READER_H
