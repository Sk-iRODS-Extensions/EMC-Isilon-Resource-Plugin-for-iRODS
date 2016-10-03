/** 
 * Tools employed by iRODS resource plugin for EMC Isilon storage
 *      
 * Copyright © 2016 by EMC Corporation, All Rights Reserved
 *  
 * This software contains the intellectual property of EMC Corporation or is licensed to
 * EMC Corporation from third parties. Use of this software and the intellectual property
 * contained therein is expressly limited to the terms and conditions of the License
 * Agreement under which it is provided by or on behalf of EMC.
 */     

#ifndef _LIBIRODS_ISILON_TOOLS_H_
#define _LIBIRODS_ISILON_TOOLS_H_

#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/locks.hpp>

// =-=-=-=-=-=-=-
// STL includes
#include <unordered_map>
#include <utility>
#include <functional>

/**
 * Hash table with thread-safety
 *
 * Built upon STL unordered_map. Table reads are non-locking.
 * Only writes are locking
 *
 * All interfaces duplicate original unordered_map interfaces
 */
template<class FIRST, class SECOND> class synchro_map
{
    private:
        boost::shared_mutex mutex;
        std::unordered_map<FIRST, SECOND> map;
        typedef class std::unordered_map<FIRST, SECOND>::const_iterator const_iter;
        typedef class std::unordered_map<FIRST, SECOND>::iterator iter;
        typedef typename std::unordered_map<FIRST, SECOND>::size_type sizet;

    public:
        std::pair<iter, bool> insert( std::pair<FIRST, SECOND>&& val)
        {
            boost::upgrade_lock<boost::shared_mutex> upgradable_lock( mutex);
            boost::upgrade_to_unique_lock<boost::shared_mutex> unique_lock( upgradable_lock);

            return map.insert( std::forward< std::pair<FIRST, SECOND> >( val));
        }

        SECOND& at( const FIRST& key)
        {
            boost::shared_lock<boost::shared_mutex> shared_lock( mutex);

            return map.at( key);
        }

        const SECOND& at( const FIRST& key) const
        {
            boost::shared_lock<boost::shared_mutex> shared_lock( mutex);

            return map.at( key);
        }

        /* Do not support operation[] since it may be used either
           for modification or reading of map */
        /* SECOND& operator[]( FIRST&& key)
        {
            return map[std::forward<FIRST>( key)];
        } */

        const_iter find( const FIRST& key) const
        {
            boost::shared_lock<boost::shared_mutex> shared_lock( mutex);

            return map.find( key);
        }

        iter find( const FIRST& key)
        {
            boost::shared_lock<boost::shared_mutex> shared_lock( mutex);

            return map.find( key);
        }

        const_iter begin() const
        {
            boost::shared_lock<boost::shared_mutex> shared_lock( mutex);

            return map.begin();
        }

        iter begin()
        {
            boost::shared_lock<boost::shared_mutex> shared_lock( mutex);

            return map.begin();
        }

        const_iter end() const
        {
            boost::shared_lock<boost::shared_mutex> shared_lock( mutex);

            return map.end();
        }

        iter end()
        {
            boost::shared_lock<boost::shared_mutex> shared_lock( mutex);

            return map.end();
        }

        const_iter erase( const_iter pos)
        {
            boost::upgrade_lock<boost::shared_mutex> upgradable_lock( mutex);
            boost::upgrade_to_unique_lock<boost::shared_mutex> unique_lock( upgradable_lock);

            return map.erase( pos);
        }

        sizet erase( const FIRST& key)
        {
            boost::upgrade_lock<boost::shared_mutex> upgradable_lock( mutex);
            boost::upgrade_to_unique_lock<boost::shared_mutex> unique_lock( upgradable_lock);

            return map.erase( key);
        }
};

/**
 * Template for object "cleaner"
 */
template<typename T, typename D = std::function<void(T&)> >
class handle
{
    private:
    	T res;
    	D del;

    public:
    	handle( const T& res, D del) : res( res), del ( del)
    	{ }

    	~handle()
    	{
    		del(res);
    	}

    	handle( const handle&) = delete;
    	handle& operator=( const handle&) = delete;
    	
    	T& get() { return this->res; }
    	D deleter() { return this->del; }
};

#endif // _LIBIRODS_ISILON_TOOLS_H
