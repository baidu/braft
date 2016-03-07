// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/12/24 15:29:34

#include "raft/node.h"
#include "raft/node_manager.h"
#include "raft/file_service.h"
#include "raft/builtin_service_impl.h"

namespace raft {

NodeManager::NodeManager() {
}

NodeManager::~NodeManager() {
}

bool NodeManager::server_exists(base::EndPoint addr) {
    BAIDU_SCOPED_LOCK(_mutex);
    if (addr.ip != base::IP_ANY) {
        base::EndPoint any_addr(base::IP_ANY, addr.port);
        if (_addr_set.find(any_addr) != _addr_set.end()) {
            return true;
        }
    }
    return _addr_set.find(addr) != _addr_set.end();
}

void NodeManager::remove_address(base::EndPoint addr) {
    BAIDU_SCOPED_LOCK(_mutex);
    _addr_set.erase(addr);
}

int NodeManager::add_service(baidu::rpc::Server* server, 
                             const base::EndPoint& listen_address) {
    if (server == NULL) {
        LOG(ERROR) << "server is NULL";
        return -1;
    }
    if (server_exists(listen_address)) {
        return 0;
    }

    if (0 != server->AddService(new FileServiceImpl, baidu::rpc::SERVER_OWNS_SERVICE)) {
        LOG(ERROR) << "Add File Service Failed.";
        return -1;
    }

    if (0 != server->AddService(
                new RaftServiceImpl(listen_address), 
                baidu::rpc::SERVER_OWNS_SERVICE)) {
        LOG(ERROR) << "Add Raft Service Failed.";
        return -1;
    }

    if (0 != server->AddService(new RaftStatImpl, baidu::rpc::SERVER_OWNS_SERVICE)) {
        LOG(ERROR) << "Add Raft Service Failed.";
        return -1;
    }
    {
        BAIDU_SCOPED_LOCK(_mutex);
        _addr_set.insert(listen_address);
    }
    return 0;
}

size_t NodeManager::_add_node(Maps& m, const NodeImpl* node) {
    NodeId node_id = node->node_id();
    std::pair<NodeMap::iterator, bool> ret = m.node_map.insert(
            NodeMap::value_type(node_id, const_cast<NodeImpl*>(node)));
    if (ret.second) {
        m.group_map.insert(GroupMap::value_type(
                    node_id.group_id, const_cast<NodeImpl*>(node)));
        return 1;
    }
    return 0;
}

size_t NodeManager::_remove_node(Maps& m, const NodeImpl* node) {
    if (m.node_map.erase(node->node_id()) != 0) {
        std::pair<GroupMap::iterator, GroupMap::iterator> 
                range = m.group_map.equal_range(node->node_id().group_id);
        for (GroupMap::iterator it = range.first; it != range.second; ++it) {
            if (it->second == node) {
                m.group_map.erase(it);
                return 1;
            }
        }
        CHECK(false) << "Can't reach here";
        return 0;
    }
    return 0;
}

bool NodeManager::add(NodeImpl* node) {
    // check address ok?
    if (!server_exists(node->node_id().peer_id.addr)) {
        return false;
    }

    return _nodes.Modify(_add_node, node) != 0;
}

bool NodeManager::remove(NodeImpl* node) {
    return _nodes.Modify(_remove_node, node) != 0;
}

scoped_refptr<NodeImpl> NodeManager::get(const GroupId& group_id, const PeerId& peer_id) {
    base::DoublyBufferedData<Maps>::ScopedPtr ptr;
    if (_nodes.Read(&ptr) != 0) {
        return NULL;
    }
    NodeMap::const_iterator it = ptr->node_map.find(NodeId(group_id, peer_id));
    if (it != ptr->node_map.end()) {
        return it->second;
    }
    return NULL;
}

void NodeManager::get_nodes_by_group_id(
        const GroupId& group_id, std::vector<scoped_refptr<NodeImpl> >* nodes) {

    nodes->clear();
    base::DoublyBufferedData<Maps>::ScopedPtr ptr;
    if (_nodes.Read(&ptr) != 0) {
        return;
    }
    std::pair<GroupMap::const_iterator, GroupMap::const_iterator> 
            range = ptr->group_map.equal_range(group_id);
    for (GroupMap::const_iterator it = range.first; it != range.second; ++it) {
        nodes->push_back(it->second);
    }
}

void NodeManager::get_all_nodes(std::vector<scoped_refptr<NodeImpl> >* nodes) {
    nodes->clear();
    base::DoublyBufferedData<Maps>::ScopedPtr ptr;
    if (_nodes.Read(&ptr) != 0) {
        return;
    }
    nodes->reserve(ptr->group_map.size());
    for (GroupMap::const_iterator 
            it = ptr->group_map.begin(); it != ptr->group_map.end(); ++it) {
        nodes->push_back(it->second);
    }
}

}  // namespace raft

