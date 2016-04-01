//
//  DiskMultiMap.cpp
//  Project_4
//
//  Created by Lucas Jenkins on 3/5/16.
//  Copyright © 2016 Lucas Jenkins. All rights reserved.
//

#include "DiskMultiMap.h"
#include <cassert>
#include <cstring>
using namespace std;

///////////////////////////////////////////////////////////////////////////
//Implementation of housekeeping functions for setting up multimaps
///////////////////////////////////////////////////////////////////////////
bool DiskMultiMap::createNew(const string& filename, unsigned int numBuckets){
    close();
    if(!m_file.createNew(filename)){
        return false;
    }
    m_nBuckets = numBuckets;
    Bucket tempBuck;
    for(int i = 0; i < numBuckets; i++){
        Offset o = HEADERSIZE + i*BUCKETSIZE;
        if(!m_file.write(tempBuck, o))
            return false;
    }
    //default constructed-->m_next and m_prev pointers are NULLOFFSET
    Node empty;
    empty.m_offset = HEADERSIZE + (numBuckets * BUCKETSIZE);
    m_file.write(empty, empty.m_offset);
    //initialize a header with the empty nodes pointer pointing to empty from above
    Header head = Header(numBuckets, empty.m_offset);
    head.m_end = empty.m_offset + NODESIZE;
    if(!m_file.write(head, 0))
        return false;
    return true;
}

bool DiskMultiMap::openExisting(const string& filename){
    m_file.close();
    if(!m_file.openExisting(filename))
       return false;
    Header h;
    m_file.read(h, 0);
    m_nBuckets = h.m_nBuckets;
    return true;
}

void DiskMultiMap::close(){
    m_file.close();
}

///////////////////////////////////////////////////////////////////////////
//Implementations of functions for interacting with a given multimap
///////////////////////////////////////////////////////////////////////////
bool DiskMultiMap::insert(const string& key, const string& value, const string& context){
    if(key.length() > 120 || value.length() > 120 || context.length() > 120)
        return false;
    //Get header file for information about empty nodes
    Header h;
    m_file.read(h, 0);
    Offset insertNodeAt = h.m_emptyNodes;//where the node to be inserted will be inserted
    //Node for searching through removed nodes,
    Node emptyNode;
    m_file.read(emptyNode, h.m_emptyNodes);
    //In the case that there is only 1 empty node, meaning there are no free spaces from removed nodes
    if(emptyNode.m_next == NULLOFFSET){
        emptyNode.m_offset = h.m_end;
        h.m_emptyNodes = emptyNode.m_offset;
        h.m_end += NODESIZE;
    }
    //If there is more than one empty node, meaning there are free spaces due to removed nodes
    else{
        m_file.read(emptyNode, emptyNode.m_next);
        emptyNode.m_prev = NULLOFFSET;
        h.m_emptyNodes = emptyNode.m_offset;
    }
    //We have updated the data in the header and empty node to reflect that the old empty node has been filled.
    if(!m_file.write(emptyNode, emptyNode.m_offset) || !m_file.write(h, 0))
        return false;
    Offset bucketInsertAt = fnvHash(key);
    Bucket b;
    m_file.read(b, bucketInsertAt);
    //Create info for new Node
    const char* ckey = key.c_str();
    const char* cvalue = value.c_str();
    const char* cContext = context.c_str();
    Node toWrite(ckey, cvalue, cContext);
    toWrite.m_offset = insertNodeAt;
    //We are going to place the head node of the bucket's linked list at the first empty node
    if(!b.push_front(this, toWrite, bucketInsertAt))
        return false;
    return true;
}

DiskMultiMap::Iterator DiskMultiMap::search(const string& key){
    Offset bucketOffset = fnvHash(key);
    Iterator iter;
    Bucket b;
    m_file.read(b, bucketOffset);
    if(b.m_listHead == NULLOFFSET)
        ;//Do nothing to iter
    
    else{
        Node checking;
        m_file.read(checking, b.m_listHead);
        do{
            if(key.compare(checking.m_key) == 0){
                Iterator temp(this, checking.m_offset);
                iter = temp;
                break;//break because we want to return the first node that has that key in the linked list
            }
        }while(checking.m_next != NULLOFFSET && m_file.read(checking, checking.m_next));
    }
    return iter;
}

int DiskMultiMap::erase(const std::string& key,	const string& value, const string& context){
    int count = 0;
    Offset bucketOffset = fnvHash(key);
    Bucket b;
    m_file.read(b, bucketOffset);
    if(b.m_listHead == NULLOFFSET)
        ;//Don't change count
    else{
        Node checking;
        m_file.read(checking, b.m_listHead);
        do{
            if(value.compare(checking.m_value) == 0 && context.compare(checking.m_context) == 0 && key.compare(checking.m_key) == 0){
                eraseNode(checking, b, bucketOffset);
                count++;
            }
        }while(checking.m_next != NULLOFFSET && m_file.read(checking, checking.m_next));
    }
    return count;
}

///////////////////////////////////////////////////////////////////////////
//Implementation of private member functions
///////////////////////////////////////////////////////////////////////////
Offset DiskMultiMap::fnvHash(const string& key){
    unsigned int hash = 2166136261u; //The u suffix tells the compiler to treat this number not as a regular integer, but as an unsigned integer
    unsigned int j = 16777619;
    for(int i = 0; i < key.size(); i++){
        hash += key[i];
        hash *= j;
    }
    return convertFNVtoBucket(hash);
}

Offset DiskMultiMap::convertFNVtoBucket(unsigned int hash){
    unsigned int rand = hash % m_nBuckets;//gives us a number between 0 and m_nBuckets, evenly distributed
    return (rand * BUCKETSIZE) + HEADERSIZE;
}

void DiskMultiMap::eraseNode(Node& toErase, Bucket erasingFrom, Offset bucketOff){
    //Connect nodes before and after toErase to each other, essentially getting rid of toErase from the linked list of the bucket
    Node temp;//temp serves as a node that will be used to change multiple nodes throughout the linked list
    if(toErase.m_next != NULLOFFSET){
        m_file.read(temp, toErase.m_next);
        //temp is now the node after toErase
        temp.m_prev = toErase.m_prev;
        m_file.write(temp, temp.m_offset);
    }
    if(toErase.m_prev != NULLOFFSET){
        m_file.read(temp, toErase.m_prev);
        //temp is now the node before toErase
        temp.m_next = toErase.m_next;
        m_file.write(temp, temp.m_offset);
    }
    //If toErase.m_prev *IS* NULLOFFSET, then toErase is the head of the list - and because we're deleting it, we therefore need to update the bucket we are erasing from
    else{
        erasingFrom.m_listHead = toErase.m_next;
        m_file.write(erasingFrom, bucketOff);
    }
    //Add toErase at the beginning of the header's linked list of empty nodes
    Header head;
    m_file.read(head, 0);
    m_file.read(temp, head.m_emptyNodes);
    //temp is now the old first empty node
    temp.m_prev = toErase.m_offset;
    toErase.m_next = head.m_emptyNodes;
    toErase.m_prev = NULLOFFSET;
    head.m_emptyNodes = toErase.m_offset;
    m_file.write(temp, temp.m_offset);
    m_file.write(toErase, toErase.m_offset);
    m_file.write(head, 0);
}

//Adds a node to the front of a given buckets linked list
bool DiskMultiMap::Bucket::push_front(DiskMultiMap* d, DiskMultiMap::Node& newNode, Offset bucketOffset){
    if(m_listHead != NULLOFFSET){
        Node oldHead;
        d->m_file.read(oldHead, m_listHead);
        oldHead.m_prev = newNode.m_offset;
        newNode.m_next = oldHead.m_offset;
        if(!d->m_file.write(oldHead, oldHead.m_offset))
            return false;
    }
    m_listHead = newNode.m_offset;
    if(!d->m_file.write(*this, bucketOffset) || !d->m_file.write(newNode, newNode.m_offset))
        return false;
    return true;
}


///////////////////////////////////////////////////////////////////////////
//Implementation of nested Iterator class
//////////////////////////////////////////////////////////////////////////

DiskMultiMap::Iterator& DiskMultiMap::Iterator::operator++(){
    if(!m_validity)
        return *this;
    char originalKey[121];
    strcpy(originalKey, m_data.m_key);
    while(m_data.m_next != NULLOFFSET){
        m_currMap->m_file.read(m_data, m_data.m_next);
        if(strcmp(m_data.m_key, originalKey) == 0)
            return *this;
    }
    strcpy(m_data.m_key, "");
    strcpy(m_data.m_value, "");
    strcpy(m_data.m_context, "");
    m_validity = false;
    return *this;
}
