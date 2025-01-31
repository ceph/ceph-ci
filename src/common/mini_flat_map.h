#pragma once

#include <iterator>
#include <vector>
#include <cstddef>
#include <memory>
#include <include/ceph_assert.h>

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
        ceph_assert((int8_t)key == map->data.size()); // end
      }
    }
    // Only for end constructor.
    iterator(const mini_flat_map *map, size_t map_size) : map(map), key((int8_t)map_size)
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
        ceph_assert((int8_t)key == map->data.size()); // end
      }
    }

    const_iterator(const mini_flat_map *map, size_t map_size) : map(map), key((int8_t)map_size)
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
  int8_t _size;

  mini_flat_map(size_t max_size) : data(max_size), _end(this, max_size), _const_end(this, max_size), _size(0) {}
  mini_flat_map(mini_flat_map &&other) noexcept : data(std::move(other.data)), _end(this, data.size()), _const_end(this, data.size()), _size(0)
  {
    for (auto &&_ : *this) _size++;
  };
  mini_flat_map(const mini_flat_map &other) noexcept : data(other.data.size()) , _end(this, data.size()), _const_end(this, data.size()), _size(0)
  {
    for (auto &&[k, t] : other) {
      emplace(k, t);
    }
    ceph_assert(_size == other._size);
  };
  mini_flat_map(size_t max_size, const std::map<Key, T> &&other) :  data(max_size), _end(this, max_size), _const_end(this, max_size), _size(0)
  {
    for (auto &&[k, t] : other) {
      emplace(k, std::move(t));
    }
    ceph_assert(_size == other.size());
  }
  bool contains(Key const &key) const
  {
    return key < data.size() && data.at(key);
  }
  [[nodiscard]] bool empty() const
  {
    return _size == 0;
  }
  void swap(mini_flat_map &other) noexcept
  {
    data.swap(other.data);
    int8_t tmp = _size;
    _size = other._size;
    other._size = tmp;
  }
  void clear()
  {
    if (!_size) return;
    for (auto &&d : data) d.reset();
    _size = 0;
  }
  mini_flat_map& operator=(mini_flat_map &&other) noexcept
  {
    data = std::move(other.data);
    _size = other._size;
    return *this;
  }
  mini_flat_map& operator=(const mini_flat_map &other)
  {
    ceph_assert(data.size() == other.data.size());
    clear();

    for (auto &&[k, v] : other) emplace(k, T(v));

    ceph_assert(_size == other._size);

    return *this;
  }

  iterator erase(iterator &i)
  {
    erase(i.key);
    i.progress();
    return i;
  }
  iterator erase(const_iterator &i)
  {
    erase(i.key);
    i.progress();
    return iterator(this, i.key);
  }
  size_t erase(const Key &s)
  {
    if(!contains(s)) return 0;
    _size--;
    data.at(s).reset();
    return 1;
  }
  const_iterator begin() const
  {
    return cbegin();
  }
  const_iterator end() const
  {
    return cend();
  }
  const_iterator cbegin() const
  {
    return const_iterator(this);
  }
  const_iterator cend() const
  {
    return _const_end;
  }
  iterator begin()
  {
    return iterator(this);
  }
  iterator end()
  {
    return _end;
  }
  auto size() const { return (size_t)_size; }
  auto max_size() const { return data.size(); }
  T& at(Key &k) { return *data.at(k); }
  T& at(Key const &k) { return *data.at(k); }
  const T& at(Key &k) const { return *data.at(k); }
  const T& at(Key const &k) const { return *data.at(k); }
  auto operator==(mini_flat_map<Key,T> const &other) const
  {
    if (_size != other._size) return false;

    for (auto &&[k, v] : *this) {
      if (!other.contains(k)) return false;
      if (other.at(k) != v) return false;
    }

    return true;
  }

  template< class... Args >
  bool emplace(Key k, Args&&... args)
  {
    if (!data[k]) {
      _size++;
      vector_type t = std::make_unique<T>(std::forward<Args>(args)...);
      data[k] = std::move(t);
      return true;
    }
    return false;
  }

  T& operator[](const Key &s)
  {
    if (!contains(s)) ceph_assert(emplace(s));
    return at(s);
  }

  size_t count( const Key& key ) const
  {
    return contains(key) ? 1 : 0;
  }

  iterator find( const Key& key )
  {
    if (!contains(key)) return _end;
    return iterator(this, key);
  }

  const_iterator find( const Key& key ) const
  {
    if (!contains(key)) return _const_end;
    return const_iterator(this, key);
  }

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
