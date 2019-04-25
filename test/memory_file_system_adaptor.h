// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved

// Author: ZhengPengFei (zhengpengfei@baidu.com)
// Date: 2017/06/16 10:35:45

#ifndef  BRAFT_MEMORY_FILE_SYSTEM_ADAPTOR_H
#define  BRAFT_MEMORY_FILE_SYSTEM_ADAPTOR_H

#include <list>
#include "braft/file_system_adaptor.h"

class MemoryDirReader : public braft::DirReader {
public:
    MemoryDirReader(const std::vector<std::string>& names) 
        : _names(names), _pos(0) {}
    virtual ~MemoryDirReader() {}

    virtual bool is_valid() const {
        return _pos <= _names.size();
    }

    virtual bool next() {
        if (_pos < _names.size()) {
            ++_pos;
            return true;
        }
        return false;
    }

    virtual const char* name() const {
        return _names[_pos - 1].c_str();
    }

private:
    std::vector<std::string> _names;
    size_t _pos;
};

struct TreeNode;
struct TreeNodeImpl : public butil::RefCountedThreadSafe<TreeNodeImpl> {
    bthread::Mutex mutex;
    bool file;
    butil::IOBuf content;
    std::list<scoped_refptr<TreeNode> > childs;
};

struct TreeNode : public butil::RefCountedThreadSafe<TreeNode> {
    std::string name;
    scoped_refptr<TreeNodeImpl> impl;
    TreeNode() {}
};

class MemoryFileAdaptor : public braft::FileAdaptor {
public:
    MemoryFileAdaptor(TreeNodeImpl* node_impl) : _node_impl(node_impl) {}

    ssize_t write(const butil::IOBuf& data, off_t offset) {
        std::unique_lock<bthread::Mutex> lck(_node_impl->mutex);
        size_t file_size = _node_impl->content.size();
        butil::IOBuf content = _node_impl->content;
        _node_impl->content.clear();

        size_t prefix_len = std::min(file_size, size_t(offset));
        if (prefix_len != 0) {
            content.cutn(&(_node_impl->content), prefix_len);
        }
        _node_impl->content.resize(offset);
        _node_impl->content.append(data);
        if (content.size() > data.size()) {
            butil::IOBuf tmp_buf;
            content.cutn(&tmp_buf, data.size());
        }
        _node_impl->content.append(content);
        return data.size();
    }

    ssize_t read(butil::IOPortal* portal, off_t offset, size_t size) {
        std::unique_lock<bthread::Mutex> lck(_node_impl->mutex);
        butil::IOBuf content = _node_impl->content;
        if (content.size() <= size_t(offset)) {
            return 0;
        }
        butil::IOBuf tmp_buf;
        content.cutn(&tmp_buf, offset);
        ssize_t readn = std::min(content.size(), size);
        content.cutn(portal, readn);
        return readn;
    }

    ssize_t size() {
        std::unique_lock<bthread::Mutex> lck(_node_impl->mutex);
        return _node_impl->content.size();
    }

    bool sync() {
        return true;
    }

    bool close() {
        return true;
    }


private:
    scoped_refptr<TreeNodeImpl> _node_impl;
};

class MemoryFileSystemAdaptor : public braft::FileSystemAdaptor {
public:
    MemoryFileSystemAdaptor() : braft::FileSystemAdaptor() {
        _root = new TreeNode;
        _root->name = butil::FilePath::kCurrentDirectory;
        _root->impl = new TreeNodeImpl;
        _root->impl->file = false;
    }

    virtual ~MemoryFileSystemAdaptor() {}

    braft::FileAdaptor* open(const std::string& path, int oflag, 
                            const ::google::protobuf::Message* file_meta,
                            butil::File::Error* e) {
        (void) file_meta;
        if (e) {
            *e = butil::File::FILE_OK;
        }
        std::unique_lock<bthread::Mutex> lck(_mutex);
        butil::FilePath p(path);
        scoped_refptr<TreeNode>* node_ptr = find_tree_node(p);
        if (node_ptr) {
            scoped_refptr<TreeNode>& node = *node_ptr;
            if (!node->impl->file) {
                if (e) {
                    *e = butil::File::FILE_ERROR_NOT_A_FILE;
                }
                return NULL;
            }
            if (oflag & O_TRUNC) {
                node->impl->content.clear();
            }
            return new MemoryFileAdaptor(node->impl.get());
        }
        if (!(oflag & O_CREAT)) {
            if (e) {
                *e = butil::File::FILE_ERROR_NOT_FOUND;
            }
            return NULL;
        }
        scoped_refptr<TreeNode>* parent_node_ptr = find_tree_node(p.DirName());
        if (!parent_node_ptr || (*parent_node_ptr)->impl->file) {
            if (e) {
                *e = butil::File::FILE_ERROR_NOT_FOUND;
            }
            return NULL;
        }
        TreeNode* node = new TreeNode;
        node->name = p.BaseName().value();
        node->impl = new TreeNodeImpl;
        node->impl->file = true;
        (*parent_node_ptr)->impl->childs.push_back(node);
        return new MemoryFileAdaptor(node->impl.get());
    }

    bool delete_file(const std::string& path, bool recursive) {
        std::unique_lock<bthread::Mutex> lck(_mutex);
        return unsafe_delete_file(path, recursive);
    }

    bool rename(const std::string& old_path, 
                const std::string& new_path) {
        std::unique_lock<bthread::Mutex> lck(_mutex);
        butil::FilePath old_p(old_path);
        butil::FilePath new_p(new_path);
        scoped_refptr<TreeNode>* old_node_ptr = find_tree_node(old_p);
        if (!old_node_ptr) {
            return false;
        }
        scoped_refptr<TreeNode>& old_node = *old_node_ptr;
        scoped_refptr<TreeNode>* new_node_ptr = find_tree_node(new_p);
        if (new_node_ptr) {
            scoped_refptr<TreeNode>& new_node = *new_node_ptr;
            if (new_node->impl->file) {
                if (old_node->impl->file) {
                    new_node->impl = old_node->impl;
                } else {
                    return false;
                }
            } else {
                if (old_node->impl->file) {
                    return false;
                } else if (!new_node->impl->childs.empty()) {
                    return false;
                } else {
                    new_node->impl = old_node->impl;
                }
            }
        } else {
            new_node_ptr = find_tree_node(new_p.DirName());
            if (!new_node_ptr || (*new_node_ptr)->impl->file) {
                return false;
            }
            new_node_ptr = create_child(*new_node_ptr, new_p.BaseName().value(), true);
            (*new_node_ptr)->impl = old_node->impl;
        }

        CHECK(unsafe_delete_file(old_path, true));
        return true;
    }

    bool link(const std::string& old_path, const std::string& new_path) {
        std::unique_lock<bthread::Mutex> lck(_mutex);
        butil::FilePath old_p(old_path);
        butil::FilePath new_p(new_path);
        scoped_refptr<TreeNode>* old_node_ptr = find_tree_node(old_p);
        if (!old_node_ptr) {
            return false;
        }
        scoped_refptr<TreeNode>* new_node_ptr = find_tree_node(new_p);
        if (new_node_ptr) {
            return false;
        }
        new_node_ptr = find_tree_node(new_p.DirName());
        if (!new_node_ptr) {
            return false;
        }
        new_node_ptr = create_child(*new_node_ptr, new_p.BaseName().value(), true);
        (*new_node_ptr)->impl = (*old_node_ptr)->impl;
        return true;
    }

    bool create_directory(const std::string& path, 
                          butil::File::Error* error,
                          bool create_parent_directories) {
        std::unique_lock<bthread::Mutex> lck(_mutex);
        return unsafe_create_directory(path, error, create_parent_directories);
    }

    bool path_exists(const std::string& path) {
        std::unique_lock<bthread::Mutex> lck(_mutex);
        butil::FilePath p(path);
        scoped_refptr<TreeNode>* node_ptr = find_tree_node(p);
        return node_ptr != NULL;
    }

    bool directory_exists(const std::string& path) {
        std::unique_lock<bthread::Mutex> lck(_mutex);
        butil::FilePath p(path);
        scoped_refptr<TreeNode>* node_ptr = find_tree_node(p);
        return node_ptr != NULL && !((*node_ptr)->impl->file);
    }

    braft::DirReader* directory_reader(const std::string& path) {
        std::unique_lock<bthread::Mutex> lck(_mutex);
        std::vector<std::string> names;
        butil::FilePath p(path);
        scoped_refptr<TreeNode>* node_ptr = find_tree_node(p);
        if (node_ptr) {
            std::list<scoped_refptr<TreeNode> >& childs = (*node_ptr)->impl->childs;
            for (std::list<scoped_refptr<TreeNode> >::iterator it = childs.begin();
                    it != childs.end(); ++it) {
                names.push_back((*it)->name);
            }
        }
        return new MemoryDirReader(names);
    }

private:

    scoped_refptr<TreeNode>* find_tree_node(const butil::FilePath& path) {
        std::vector<butil::FilePath> subpaths;
        subpaths.push_back(path.BaseName());
        int ignore = 0;
        for (butil::FilePath sub_path = path.DirName();
                sub_path.value() != _root->name; sub_path = sub_path.DirName()) {
            if (sub_path.BaseName().value() == "/" || sub_path.BaseName().value() == ".") {
                continue;
            } else if (sub_path.BaseName().value() == "..") {
                ++ignore;
            } else if (ignore > 0) {
                --ignore;
            } else {
                subpaths.push_back(sub_path.BaseName());
            }
        }
        scoped_refptr<TreeNode>* node = &_root;
        std::vector<butil::FilePath>::reverse_iterator i;
        for (i = subpaths.rbegin(); i != subpaths.rend(); ++i) {
            if (i->value() == "/") {
                continue;
            }
            if (i->value() == ".") {
                continue;
            }
            bool found = false;
            std::list<scoped_refptr<TreeNode> >& childs = (*node)->impl->childs;
            for (std::list<scoped_refptr<TreeNode> >::iterator sub_it = childs.begin(); 
                    sub_it != childs.end(); ++sub_it) {
                if (i->value() == (*sub_it)->name) {
                    found = true;
                    node = &(*sub_it);
                    break;
                }
            }
            if (!found) {
                return NULL;
            }
        }
        return node;
    }

    bool unsafe_delete_file(const std::string& path, bool recursive) {
        butil::FilePath p(path);
        scoped_refptr<TreeNode>* node_ptr = find_tree_node(p);
        if (!node_ptr) {
            return true;
        }
        scoped_refptr<TreeNode>& node = *node_ptr;
        if (!node->impl->file && !recursive && !node->impl->childs.empty()) {
            return false;
        }
        scoped_refptr<TreeNode>& parent_node = *(find_tree_node(p.DirName()));
        for (std::list<scoped_refptr<TreeNode> >::iterator it = parent_node->impl->childs.begin(); 
                it != parent_node->impl->childs.end(); ++it) {
            if ((*it).get() == node.get()) {
                parent_node->impl->childs.erase(it);
                return true;
            }
        }
        CHECK(false);
        return false;
    }

    scoped_refptr<TreeNode>* create_child(scoped_refptr<TreeNode>& parent,
                                          const std::string& name, 
                                          bool file) {
        scoped_refptr<TreeNode>* node = NULL;
        parent->impl->childs.push_back(new TreeNode);
        node = &(parent->impl->childs.back());
        (*node)->name = name;
        (*node)->impl = new TreeNodeImpl;
        (*node)->impl->file = file;
        return node;
    }

    bool unsafe_create_directory(const std::string& path, 
                                 butil::File::Error* error,
                                 bool create_parent_directories) {
        if (error) {
            *error = butil::File::FILE_OK;
        }
        butil::FilePath p(path);
        std::vector<butil::FilePath> subpaths;
        subpaths.push_back(p.BaseName());
        int ignore = 0;
        for (butil::FilePath sub_path = p.DirName();
                sub_path.value() != _root->name; sub_path = sub_path.DirName()) {
            if (sub_path.BaseName().value() == "/" || sub_path.BaseName().value() == ".") {
                continue;
            } else if (sub_path.BaseName().value() == "..") {
                ++ignore;
            } else if (ignore > 0) {
                --ignore;
            } else {
                subpaths.push_back(sub_path.BaseName());
            }
        }
        scoped_refptr<TreeNode>* node = &_root;
        std::vector<butil::FilePath>::reverse_iterator i;
        for (i = subpaths.rbegin(); i != subpaths.rend(); ++i) {
            if (i->value() == "/") {
                continue;
            }
            if (i->value() == ".") {
                continue;
            }
            bool found = false;
            std::list<scoped_refptr<TreeNode> >& childs = (*node)->impl->childs;
            for (std::list<scoped_refptr<TreeNode> >::iterator sub_it = childs.begin(); 
                    sub_it != childs.end(); ++sub_it) {
                if (i->value() == (*sub_it)->name) {
                    found = true;
                    node = &(*sub_it);
                    break;
                }
            }
            if (found) {
                continue;
            } else {
                break;
            }
        }
        if (i == subpaths.rend()) {
            bool ret = !(*node)->impl->file;
            if (!ret && error) {
                *error = butil::File::FILE_ERROR_EXISTS;
            }
            return ret;
        }
        if (!create_parent_directories && (i + 1) != subpaths.rend()) {
            if (error) {
                *error = butil::File::FILE_ERROR_NOT_FOUND;
            }
            return false;
        }
        for (; i != subpaths.rend(); ++i) {
            (*node)->impl->childs.push_back(new TreeNode);
            node = &((*node)->impl->childs.back());
            (*node)->name = (*i).value();
            (*node)->impl = new TreeNodeImpl;
            (*node)->impl->file = false;
        }
        return true;
    }

    bthread::Mutex _mutex;
    scoped_refptr<TreeNode> _root;
};

#endif // BRAFT_MEMORY_FILE_SYSTEM_ADAPTOR_H
