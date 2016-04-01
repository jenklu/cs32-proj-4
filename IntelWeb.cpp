//
//  IntelWeb.cpp
//  Project_4
//
//  Created by Lucas Jenkins on 3/5/16.
//  Copyright © 2016 Lucas Jenkins. All rights reserved.
//

#include "IntelWeb.h"
#include "DiskMultiMap.h"
#include "MultiMapTuple.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <queue>

using namespace std;

bool IntelWeb::createNew(const std::string& filePrefix, unsigned int maxDataItems){
    close();
    unsigned int numBuckets = maxDataItems*(5/3);
    if(!actorIsKey.createNew(filePrefix + "-actorIsKey.dat", numBuckets))
        return false;
    if(!targetIsKey.createNew(filePrefix + "-targetIsKey.dat", numBuckets)){
        actorIsKey.close();
        return false;
    }
    return true;
}

bool IntelWeb::openExisting(const std::string& filePrefix){
    close();
    if(!actorIsKey.openExisting(filePrefix + "-actorIsKey.dat"))
        return false;
    
    if(!targetIsKey.openExisting(filePrefix + "-targetIsKey.dat")){
        actorIsKey.close();
        return false;
    }
    return true;
}

bool IntelWeb::ingest(const string& telemetryFile){
    ifstream input(telemetryFile);
    if(!input)
        return false;
    string line;
    while(getline(input, line)){
        istringstream helper(line);
        string context, actor, target;
        if(! (helper >> context >> actor >> target)){
            continue;
        }
        actorIsKey.insert(actor, target, context);
        targetIsKey.insert(target, actor, context);
    }
    return true;
}

unsigned int IntelWeb::crawl(const vector<string>& indicators, unsigned int minPrevalenceToBeGood,
                   vector<std::string>& badEntitiesFound, vector<InteractionTuple>& interactions){
    set<string> badEntitySet;
    unordered_set<string> searchedBadEnts;
    unordered_map<string, bool> entityIsPrev;
    
    //finding bad entities
    ////////////////////////////////////////////////////////////////////////////
    for(auto indIter = indicators.begin(); indIter != indicators.end(); ++indIter){

        //const string& badEnt, set<string>& badset, unordered_set<string>& searched,  unordered_map<string, bool>& storedPrevs, unsigned int minPrev
        followBadEnt(*indIter, badEntitySet, searchedBadEnts, entityIsPrev, minPrevalenceToBeGood);
    }
    badEntitiesFound.assign(badEntitySet.begin(), badEntitySet.end());
    
    ////////////////////////////////////////////////////////////////////////////
    //code below is probably ok
    set<TupleWrapper> badInteractions;
    for(auto vectIter = badEntitiesFound.begin(); vectIter != badEntitiesFound.end(); ++vectIter){
        auto actorIter = actorIsKey.search(*vectIter);
        auto targetIter = targetIsKey.search(*vectIter);
        MultiMapTuple temp;
        for( ; actorIter.isValid(); ++actorIter){
            temp = *actorIter;
            badInteractions.insert(TupleWrapper(temp.key, temp.value, temp.context));
        }
        for( ; targetIter.isValid(); ++targetIter){
            temp = *targetIter;
            badInteractions.insert(TupleWrapper(temp.value, temp.key, temp.context));
        }
    }
    interactions.clear();
    for(auto setIter = badInteractions.begin(); setIter != badInteractions.end(); ++setIter){
        interactions.push_back((*setIter).m_tuple);
    }
    return badEntitiesFound.size();
}

bool IntelWeb::purge(const std::string& entity){
    bool toReturn = false;
    auto iter = actorIsKey.search(entity);
    if(iter.isValid())
        toReturn = true;
    for( ; iter.isValid(); ++iter){
        MultiMapTuple mmt = *iter;
        actorIsKey.erase(mmt.key, mmt.value, mmt.context);
        targetIsKey.erase(mmt.value, mmt.key, mmt.context);
    }
    iter = targetIsKey.search(entity);
    if(iter.isValid())
        toReturn = true;
    for( ; iter.isValid(); ++iter){
        MultiMapTuple mmt = *iter;
        targetIsKey.erase(mmt.key, mmt.value, mmt.context);
        actorIsKey.erase(mmt.value, mmt.key, mmt.context);
    }
    return toReturn;
}

bool IntelWeb::isPrevalent(const string& checking, unsigned int minPrevalence, unordered_map<string, bool>& storedPrevalences){
    if(storedPrevalences.find(checking) != storedPrevalences.end())
        return (storedPrevalences.find(checking))->second;
    //otherwise, we have to read from disk memory
    size_t countOccur = 0;
    auto iter = actorIsKey.search(checking);
    for( ; iter.isValid(); ++iter){
        countOccur++;
        if(countOccur >= minPrevalence){
            storedPrevalences.insert(make_pair(checking, true));
            return true;
        }
    }
    iter = targetIsKey.search(checking);
    for( ; iter.isValid(); ++iter){
        countOccur++;
        if(countOccur >= minPrevalence){
            storedPrevalences.insert(make_pair(checking, true));
            return true;
        }
    }
    storedPrevalences.insert(make_pair(checking, false));
    return false;
}

void IntelWeb::followBadEnt(const string& badEnt, set<string>& badset, unordered_set<string>& searched,  unordered_map<string, bool>& storedPrevs, unsigned int minPrev){
    queue<string> badqueue;
    badqueue.push(badEnt);
    searched.insert(badEnt);
    string currEnt;
    bool containsCurrEnt;
    while(!badqueue.empty()){
        currEnt = badqueue.front();
        badqueue.pop();
        MultiMapTuple temp;
        containsCurrEnt = false;
        
        auto actorIter = actorIsKey.search(currEnt);
        for( ; actorIter.isValid(); ++actorIter){
            containsCurrEnt = true;
            temp = *actorIter;
            if(searched.find(temp.value) == searched.end() && !isPrevalent(temp.value, minPrev, storedPrevs)){
                badqueue.push(temp.value);
                badset.insert(temp.value);
                searched.insert(temp.value);
            }
        }
        auto targetIter = targetIsKey.search(currEnt);
        for( ; targetIter.isValid(); ++targetIter){
            containsCurrEnt = true;
            temp = *targetIter;
            if(searched.find(temp.value) == searched.end() && !isPrevalent(temp.value, minPrev, storedPrevs)){
                badqueue.push(temp.value);
                badset.insert(temp.value);
                searched.insert(temp.value);
            }
        }
        if(containsCurrEnt)
            badset.insert(currEnt);
    }
}