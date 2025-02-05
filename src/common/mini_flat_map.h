// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2024 IBM
 *
 * Author: Alex Ainscow
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once

#include <vector>
#include <cstddef>
#include <memory>
#include <include/ceph_assert.h>
#include <common/bitset_set.h>

/* This class struct provides an API similar to std::map, but with the
 * restriction that "Key" must cast to/from int8_t without ambiguity. For
 * example, the key could be a simple wrapper for an int8_t, used to provide
 * some type safety.  Additionally, the constructor must be passed the max
 * value of the key, referred to as max_size.
 *
 * The structure is a vector of unique_ptrs, indexed by the key. This therefore
 * provides O(1) lookup, with an extremely low constant overhead. The size
 * reflects the number of non-null unique_ptrs, which is tracked independently.
 *
 * This was written generically, but with a single purpose in mind (in Erasure
 * Coding), so the interface is not as complete as it could be.
 */
template <typename Key, typename T>
struct mini_flat_map
{
  using vector_type = std::unique_ptr<T>;
  struct iterator
  {
    const mini_flat_map *map;
    std::optional<std::pair<const Key&, T&>> value;
    Key key;

    void progress()
    {
      while (key < map->data.size() && !map->data[key]) {
        ++key;
      }

      if (key < map->data.size()) {
        value.emplace(key, *map->data[key]);
      } else {
        value.reset();
      }
    }

    using difference_type = std::ptrdiff_t;

    iterator(const mini_flat_map *map) : map(map), key(0)
    {
      progress();
    }

    iterator(const mini_flat_map *map, Key key) : map(map), key(key)
    {
      if (key < map->data.size()) {
        value.emplace(key, *map->data[key]);
      } else {
        ceph_assert(int8_t(key) == map->data.size()); // end
      }
    }
    // Only for end constructor.
    iterator(const mini_flat_map *map, size_t map_size) : map(map), key(map_size)
    {
      ceph_assert(map_size == map->data.size());
    }

    iterator& operator++()
    {
      ++key;
      progress();
      return *this;
    }
    iterator operator++(int)
    {
      iterator tmp(*this);
      this->operator++();
      return tmp;
    }
    bool operator==(const iterator &other) const
    {
      return key == other.key && map == other.map;
    }

    std::pair<const Key&, T&>& operator*()
    {
      return *value;
    }

    std::pair<const Key&, T&>* operator->()
    {
      return value.operator->();
    }

    iterator& operator=(const iterator &other) {
      if (this != &other) {
        key = other.key;
        progress(); // populate value
      }
      return *this;
    }
  };
  static_assert(std::input_or_output_iterator<iterator>);

  struct const_iterator
  {
    const mini_flat_map *map;
    std::optional<const std::pair<const Key&, T&>> value;
    Key key;

    void progress()
    {
      while (key < map->data.size() && !map->data[key]) {
        ++key;
      }

      if (key < map->data.size()) {
        value.emplace(key, *map->data[key]);
      } else {
        value.reset();
      }
    }

    using difference_type = std::ptrdiff_t;

    const_iterator(const mini_flat_map *map) : map(map), key(0)
    {
      progress();
    }

    const_iterator(const mini_flat_map *map, Key key) : map(map), key(key)
    {
      if (key < map->data.size()) {
        value.emplace(key, *map->data[key]);
      } else {
        ceph_assert(key == map->data.size()); // end
      }
    }

    const_iterator(const mini_flat_map *map, size_t map_size) : map(map), key(map_size)
    {
      ceph_assert(map_size == map->data.size());
    }

    const_iterator& operator++()
    {
      ++key;
      progress();
      return *this;
    }
    const_iterator operator++(int)
    {
      const_iterator tmp = *this;
      this->operator++();
      return tmp;
    }
    bool operator==(const const_iterator &other) const
    {
      return key == other.key && map == other.map;
    }

    const std::pair<const Key&, T&>& operator*() const
    {
      return *value;
    }

    const std::pair<const Key&, T&>* operator->()
    {
      return value.operator->();
    }

    const_iterator& operator=(const const_iterator &other) {
      if (this != &other) {
        key = other.key;
        progress();
      }
      return *this;
    }
  };
  static_assert(std::input_or_output_iterator<const_iterator>);

  std::vector<vector_type> data;
  const iterator _end;
  const const_iterator _const_end;
  size_t _size;

  /** Basic constructor. The mini_flat_map cannot be re-sized, so there is no
   * default constructor.
   */
  mini_flat_map(size_t max_size) : data(max_size), _end(this, max_size), _const_end(this, max_size), _size(0) {}
  /** Move constructor, forwards the move to the vector
   * This has O(N) complexity.
   */
  mini_flat_map(mini_flat_map &&other) noexcept : data(std::move(other.data)), _end(this, data.size()), _const_end(this, data.size()), _size(0)
  {
    for (auto &&_ : *this) _size++;
  };
  /** Generic initializer iterator constructor, simlar to std::map constructor
   * of the same name.
   */
  template<class InputIt>
  mini_flat_map( size_t max_size, const InputIt first, const InputIt last ) : mini_flat_map(max_size)
  {
    for (InputIt it = first; it != last; ++it) {
      const Key &k(it->first);
      auto &args = it->second;
      emplace(k, args);
    }
  }

  /** Copy constructor. Forwards the copy onto the vector */
  mini_flat_map(const mini_flat_map &other) noexcept : mini_flat_map(other.data.size(), other.begin(), other.end())
  {
    ceph_assert(_size == other._size);
  };

  /** Map compatibility. Some legacy code required conversion from std::map.
   * This is similar to the move constructor
   */
  mini_flat_map(size_t max_size, const std::map<Key, T> &&other) :  data(max_size), _end(this, max_size), _const_end(this, max_size), _size(0)
  {
    for (auto &&[k, t] : other) {
      emplace(k, std::move(t));
    }
    ceph_assert(_size == other.size());
  }
  /** Map compatibility. Some legacy code required conversion from std::map.
   * This is similar to the copy constructor
   */  mini_flat_map(size_t max_size, const std::map<int, T> &other) :  mini_flat_map(max_size, other.begin(), other.end())
  {
    ceph_assert(_size == other.size());
  }

  /** Checks if there is an element with key equivalent to key in the container.
   * @param key that may be contained
   */
  bool contains(const Key& key) const
  {
    return key < data.size() && data.at(key);
  }

  /** Checks if the container has no elements. */
  [[nodiscard]] bool empty() const
  {
    return _size == 0;
  }

  /** Exchanges the contents of the container with those of other. Does not
   * invoke any move, copy, or swap operations on individual elements.
   *
   * @param other - map to be modified
   */
  void swap(mini_flat_map &other) noexcept
  {
    data.swap(other.data);
    int8_t tmp = _size;
    _size = other._size;
    other._size = tmp;
  }

  /** Erases all elements from the container. */
  void clear()
  {
    if (!_size) return;
    for (auto &&d : data) d.reset();
    _size = 0;
  }

  /** Assignment with move operator */
  mini_flat_map& operator=(mini_flat_map &&other) noexcept
  {
    data = std::move(other.data);
    _size = other._size;
    return *this;
  }

  /** Assignment with copy operator */
  mini_flat_map& operator=(const mini_flat_map &other)
  {
    ceph_assert(data.size() == other.data.size());
    clear();

    for (auto &&[k, v] : other) emplace(k, T(v));

    ceph_assert(_size == other._size);

    return *this;
  }

  /** Removes specified element from the container.
   * @param i - iterator to remove
   * @return iterator - pointing at next element (or end)
   */
  iterator erase(iterator &i)
  {
    erase(i.key);
    i.progress();
    return i;
  }
  /** Removes specified element from the container.
   * NOTE: returns iterator, rather than const_iterator as per std::map::erase
   * @param i - const_iterator to remove
   * @return iterator - pointing at next element (or end)
   */
  iterator erase(const_iterator &i)
  {
    erase(i.key);
    i.progress();
    return iterator(this, i.key);
  }

  /** Removes specified element from the container.
   * @param k - key to remove
   * @return size_t - 1 if element removed, 0 otherwise.
   */
  size_t erase(const Key &k)
  {
    if(!contains(k)) return 0;
    _size--;
    data.at(k).reset();
    return 1;
  }

  /** @return begin const_iterator */
  const_iterator begin() const
  {
    return cbegin();
  }
  /** @return end const_iterator */
  const_iterator end() const
  {
    return cend();
  }
  /** @return begin const_iterator */
  const_iterator cbegin() const
  {
    return const_iterator(this);
  }
  /** @return end const_iterator */
  const_iterator cend() const
  {
    return _const_end;
  }
  /** @return begin iterator */
  iterator begin()
  {
    return iterator(this);
  }
  /** @return end iterator */
  iterator end()
  {
    return _end;
  }
  /** return number of elements in map, This is the number of unique_ptrs
   * which are not null in the map.
   * @return size_t size
   */
  size_t size() const { return _size; }

  /** return maximum number of elements that container can hold.
   * @return size_t
   */
  auto max_size() const { return data.size(); }

  /** Returns a reference to the mapped value of the element with specified key.
   * If no such element exists, an exception of type std::out_of_range is thrown.
   *
   * @param k - key
   * @return reference to value.
   */
  T& at(const Key &k)
  {
    if (!contains(k)) throw std::out_of_range("Key not found");
    return *data.at(k);
  }
  /** Returns a reference to the mapped value of the element with specified key.
   * If no such element exists, an exception of type std::out_of_range is thrown.
   *
   * @param k - const key
   * @return const reference to value.
   */
  const T& at(const Key &k) const
  {
    if (!contains(k)) throw std::out_of_range("Key not found");
    return *data.at(k);
  }

  /** Equality operator */
  bool operator==(mini_flat_map const &other) const
  {
    if (_size != other._size) return false;

    for (auto &&[k, v] : *this) {
      if (!other.contains(k)) return false;
      if (other.at(k) != v) return false;
    }

    return true;
  }

  /** Inserts a new element into the container constructed in-place with the
   * given args, if there is no element with the key in the container.
   *
   * The constructor of the new element is called with exactly the same
   * arguments as supplied to emplace, forwarded via std::forward<Args>(args)....
   * The element may be constructed even if there already is an element with the
   * key in the container, in which case the newly constructed element will be
   * destroyed immediately (see try_emplace() if this behavior is undesirable).
   *
   * This is different to the std::map interface, in that key must be
   * provided explicitly, rather than constructed. THis does provide some
   * performance gains and should have the same behaviour.
   *
   * Careful use of emplace allows the new element to be constructed while
   * avoiding unnecessary copy or move operations.
   *
   * This also differs to std::map in that no iterators are returned
   *
   * @param k - key to add
   * @param args to construct value.
   *
   * @return true if inserted.
   */
  template< class... Args >
  bool emplace(const Key &k, Args&&... args)
  {
    ceph_assert(k < max_size());
    if (!data[k]) {
      _size++;
      vector_type t = std::make_unique<T>(std::forward<Args>(args)...);
      data[k] = std::move(t);
      return true;
    }
    return false;
  }

  /** Inserts an element into the container using the copy operator */
  bool insert(const Key &k, const T &value)
  {
    if (!data[k]) {
      _size++;
      vector_type t = std::make_unique<T>(value);
      data[k] = std::move(t);
      return true;
    }
    return false;
  }

  /** Returns a reference to the value that is mapped to a key equivalent to key,
   * performing an insertion if such key does not already exist.
   *
   * Since the key is not stored explicitly, there is no "move" variant as
   * there is in std::map.
   */
  T& operator[](const Key &s)
  {
    if (!contains(s)) ceph_assert(emplace(s));
    return at(s);
  }

  /** Returns the number of elements with key that compares equivalent to the
   * specified argument. Each key can only exist once, so cannot return more
   * than 1.
   */
  size_t count( const Key& key ) const
  {
    return contains(key) ? 1 : 0;
  }

  /** Returns an iterator to the specified key or end if it does not exist.
   * O(1) search with low overahead.
   * @param key
   * * @return iterator.
   */
  iterator find( const Key& key )
  {
    if (!contains(key)) return _end;
    return iterator(this, key);
  }

  /** Returns a const_iterator to the specified key or end if it does not exist.
   * O(1) search with low overahead.
   * @param key
   * @return const_iterator.
   */
  const_iterator find( const Key& key ) const
  {
    if (!contains(key)) return _const_end;
    return const_iterator(this, key);
  }

  /** Returns a bitset_set containing every key that exists in the map.
   *
   * @return bitset_set
   */
  bitset_set<128, Key> get_bitset_set()
  {
    bitset_set<128, Key> set;
    for (auto &&[k, _] : *this) {
      set.insert(k);
    }
    return set;
  }

  /** Standard ostream operator */
  friend std::ostream& operator<<(std::ostream& lhs, const mini_flat_map<Key,T>& rhs)
  {
    int c = 0;
    lhs << "{";
    for (auto &&[k, v] : rhs) {
      lhs << k << ":" << v;
      c++;
      if (c < rhs._size) lhs << ",";
    }
    lhs << "}";
    return lhs;
  }
};
