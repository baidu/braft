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

baidu::rpc::Server* NodeManager::get_server(const base::EndPoint& ip_and_port) {
    BAIDU_SCOPED_LOCK(_mutex);
    ServerMap::iterator it = _servers.find(ip_and_port);
    if (it != _servers.end()) {
        return it->second;
    } else {
        for (it = _servers.begin(); it != _servers.end(); ++it) {
            base::EndPoint address = it->first;
            if (address.port == ip_and_port.port &&
                (address.ip == base::IP_ANY || address.ip == ip_and_port.ip)) {
                return it->second;
            }
        }
        return NULL;
    }
}

void NodeManager::add_server(const base::EndPoint& ip_and_port, baidu::rpc::Server* server) {
    BAIDU_SCOPED_LOCK(_mutex);
    _servers.insert(std::pair<base::EndPoint, baidu::rpc::Server*>(ip_and_port, server));
}

baidu::rpc::Server* NodeManager::remove_server(const base::EndPoint& ip_and_port) {
    BAIDU_SCOPED_LOCK(_mutex);
    ServerMap::iterator it = _servers.find(ip_and_port);
    if (it == _servers.end()) {
        for (it = _servers.begin(); it != _servers.end(); ++it) {
            base::EndPoint address = it->first;
            if (address.port == ip_and_port.port &&
                (address.ip == base::IP_ANY || address.ip == ip_and_port.ip)) {
                break;
            }
        }
    }
    baidu::rpc::Server* server = NULL;
    if (it != _servers.end()) {
        server = it->second;
        _servers.erase(it);
    }
    return server;
}

int NodeManager::start(const base::EndPoint& ip_and_port,
                      baidu::rpc::Server* server, baidu::rpc::ServerOptions* options) {
    bool own = false;
    if (!server) {
        own = true;
        server = new baidu::rpc::Server;
    } else if (0 != server->listen_address().port ||
               NULL != get_server(ip_and_port)){
        LOG(ERROR) << "Add Raft Server has inited.";
        return EINVAL;
    }

    baidu::rpc::ServerOptions server_options;
    if (options) {
        server_options = *options;
    }
    if (0 != server->AddService(new FileServiceImpl, baidu::rpc::SERVER_OWNS_SERVICE)) {
        LOG(ERROR) << "Add File Service Failed.";
        return EINVAL;
    }
    if (0 != server->AddService(&_service_impl, baidu::rpc::SERVER_DOESNT_OWN_SERVICE)) {
        LOG(ERROR) << "Add Raft Service Failed.";
        return EINVAL;
    }
    if (0 != server->AddService(new RaftStatImpl, baidu::rpc::SERVER_OWNS_SERVICE)) {
        LOG(ERROR) << "Add Raft Service Failed.";
        return EINVAL;
    }
    if (0 != server->Start(ip_and_port, &server_options)) {
        LOG(ERROR) << "Start Raft Server Failed.";
        return EINVAL;
    }

    base::EndPoint address = server->listen_address();
    LOG(WARNING) << "start raft server " << address;
    add_server(ip_and_port, server);
    if (own) {
        BAIDU_SCOPED_LOCK(_mutex);
        _own_servers.insert(ip_and_port);
    }
    return 0;
}

baidu::rpc::Server* NodeManager::stop(const base::EndPoint& ip_and_port) {
    baidu::rpc::Server* server = remove_server(ip_and_port);
    if (server) {
        bool own = false;
        {
            BAIDU_SCOPED_LOCK(_mutex);
            if (_own_servers.end() != _own_servers.find(server->listen_address())) {
                _own_servers.erase(server->listen_address());
                own = true;
            }
        }
        if (own) {
            delete server; // ~Server() call Stop(0) and Join()
            server = NULL;
        }
    }

    return server;
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
    if (NULL == get_server(node->node_id().peer_id.addr)) {
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

