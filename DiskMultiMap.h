//
//  DiskMultiMap.hpp
//  Project_4
//
//  Created by Lucas Jenkins on 3/5/16.
//  Copyright © 2016 Lucas Jenkins. All rights reserved.
//

#ifndef DISKMULTIMAP_H
#define DISKMULTIMAP_H
#include "MultiMapTuple.h"
#include "BinaryFile.h"
#include <cstdint> // if Offset is int32_t instead of ios::streamoff
#include <cstring>
typedef int32_t Offset;
const Offset NULLOFFSET = -1;

class DiskMultiMap
{
 private:
    struct Node{
        Node(const char* key, const char* value, const char* context)
        {
            strcpy(m_key, key);
            strcpy(m_value, value);
            strcpy(m_context, context);
            m_offset = m_next = m_prev = NULLOFFSET;
        }
        Node(){
            m_offset = m_next = m_prev = NULLOFFSET;
        }
        char m_key[121];
        char m_value[121];
        char m_context[121];
        Offset m_offset, m_next, m_prev;
    };
    
    const int NODESIZE = sizeof(Node);
    
    struct Header{
        Header(unsigned int buckets = 0, Offset emptyNodes = NULLOFFSET)
        :m_nBuckets(buckets), m_emptyNodes(emptyNodes){}
        
        unsigned int m_nBuckets;
        Offset m_emptyNodes;
        Offset m_end;
    };
    
    const int HEADERSIZE = sizeof(Header);
    
    struct Bucket{
        Bucket(){
            m_listHead = NULLOFFSET;
        }
        bool push_front(DiskMultiMap* d, Node& newNode, Offset bucketOffset);
        Offset m_listHead;
    };
    
    const int BUCKETSIZE = sizeof(Bucket);
    
    Offset fnvHash(const std::string& key);
    Offset convertFNVtoBucket(unsigned int hash);
    void eraseNode(Node& toErase, Bucket erasingFrom, Offset bucketOff);
    
    BinaryFile m_file;
    unsigned int m_nBuckets;
    
 public:
    //	You	must	implement	this	public	nested	DiskMultiMap::Iterator	type
    class Iterator
    {
     public:
        Iterator();		//	You	must	have	a	default	constructor
        Iterator(DiskMultiMap* currMap, Offset listStart);
        bool isValid()	const;
        Iterator&	operator++();
        MultiMapTuple operator*();
     private:
        bool m_validity;
        //The Node in this data structure will NOT be written to the disk, and will simply be a COPY of a node in the disc - not an actual node in the disc
        Node m_data;
        DiskMultiMap* m_currMap;
    };
    DiskMultiMap();
    ~DiskMultiMap();
    bool createNew(const std::string& filename,	unsigned int numBuckets);
    bool openExisting(const std::string& filename);
    void close();
    bool insert(const std::string& key,	const std::string& value, const std::string& context);
    Iterator search(const std::string& key);
    int erase(const std::string& key,	const std::string& value, const std::string& context);
};

inline
DiskMultiMap::DiskMultiMap() {}

inline
DiskMultiMap::~DiskMultiMap(){
    m_file.close();
}

inline
DiskMultiMap::Iterator::Iterator(){
    m_validity = false;
    m_data = Node("", "", "");
}

inline
DiskMultiMap::Iterator::Iterator(DiskMultiMap* currMap, Offset listStart){
    m_validity = true;
    m_currMap = currMap;
    m_currMap->m_file.read(m_data, listStart);
}

inline
bool DiskMultiMap::Iterator::isValid() const{
    return m_validity;
}

inline
MultiMapTuple DiskMultiMap::Iterator::operator*(){
    MultiMapTuple toReturn;
    toReturn.key = m_data.m_key;
    toReturn.value = m_data.m_value;
    toReturn.context = m_data.m_context;
    return toReturn;
}

#endif /* DISKMULTIMAP_H */
