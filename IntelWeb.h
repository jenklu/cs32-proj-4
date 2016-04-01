//
//  IntelWeb.hpp
//  Project_4
//
//  Created by Lucas Jenkins on 3/5/16.
//  Copyright © 2016 Lucas Jenkins. All rights reserved.
//

#ifndef INTELWEB_H
#define INTELWEB_H
#include "InteractionTuple.h"
#include "DiskMultiMap.h"
#include <string>
#include <vector>
#include <set>
#include <unordered_map>
#include <unordered_set>

class IntelWeb
{
public:
    IntelWeb();
    ~IntelWeb();
    bool createNew(const std::string& filePrefix, unsigned int maxDataItems);
    bool openExisting(const std::string& filePrefix);
    void close();
    bool ingest(const std::string& telemetryFile);
    unsigned int crawl(const std::vector<std::string>& indicators,
                       unsigned int minPrevalenceToBeGood,
                       std::vector<std::string>& badEntitiesFound,
                       std::vector<InteractionTuple>& interactions
                       );
    bool purge(const std::string& entity);
    
private:
    struct TupleWrapper{
        TupleWrapper(const std::string& from, const std::string& to, const std::string& context)
        :m_tuple(from, to, context) {}
        
        bool operator<(const TupleWrapper& rhs) const;
        InteractionTuple m_tuple;
    };
    DiskMultiMap actorIsKey;
    DiskMultiMap targetIsKey;
    
    bool isPrevalent(const std::string& checking, unsigned int minPrevalence, unordered_map<string, bool>& storedPrevalences);
    bool doesContain(const std::string& entity);
    void followBadEnt(const string& badEnt, set<string>& badset, unordered_set<string>& searched,  unordered_map<string, bool>& storedPrevs, unsigned int minPrev);
};

inline
IntelWeb::IntelWeb() {}

inline
IntelWeb::~IntelWeb() { close(); }

inline
void IntelWeb::close(){
    actorIsKey.close();
    targetIsKey.close();
}

inline
bool IntelWeb::TupleWrapper::operator<(const TupleWrapper& rhs) const{
    InteractionTuple left = this->m_tuple;
    InteractionTuple right = rhs.m_tuple;
    if(left.context < right.context)
        return true;
    else if(left.context > right.context)
        return false;
    else if(left.from < right.from)
        return true;
    else if(left.from > right.from)
        return false;
    else if(left.to < right.to)
        return true;
    return false;
}

#endif /* INTELWEB_H */
