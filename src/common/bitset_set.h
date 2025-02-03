//
/* The standard bitset library does not behave like a "std::set", making it
 * hard to use as a drop-in replace.  This templated class is intended to behave
 * like a std::set() with the restriction that it can only store values less
 * than N. The contents is stored as a bitset for efficiency and there are some
 * extensions (such as insert_range) that exploit this implementations, however
 * the main intent is that this is a drop-in replacement for std::set.
 *
 * The Key type provided must implement pre-increment and cast-to-int operators.
 * Any int casts should sufficient.
*/

#pragma once
#include <cstdint>

#include "include/buffer.h"

template<size_t N, typename Key>
class bitset_set {
    enum {
    BITS_PER_UINT64 = 64,
    WORDS = N / 64,
    MAX = N
  };

  // N must be a multiple of 64
  static_assert(N % BITS_PER_UINT64 == 0);

  // Not possible to create a non-const iterator!
public:
  class const_iterator {
    const bitset_set *set;
    Key pos;

  public:
    using difference_type = std::int64_t;

    const_iterator() : set(nullptr), pos(0) {}
    const_iterator(const bitset_set *_set, size_t _pos) : set(_set), pos(_pos) {}

    const_iterator(const bitset_set *_set) : set(_set), pos(MAX) {
      for (size_t i = 0; i < WORDS; ++i) {
        size_t p = std::countr_zero(set->words[i]);
        if (p != BITS_PER_UINT64) {
          pos = (i*BITS_PER_UINT64) + p;
          break;
        }
      }
    }

    const_iterator& operator++() {
      uint64_t v;
      size_t i = (pos + 1) / BITS_PER_UINT64;
      int bit = (pos + 1) % BITS_PER_UINT64;
      while (i < WORDS) {
        if (bit == BITS_PER_UINT64) {
          v = set->words[i];
        } else {
          v = set->words[i] & -1UL << bit;
        }
        bit = std::countr_zero(v);
        if (bit != BITS_PER_UINT64) {
          pos = (i * BITS_PER_UINT64) + bit;
          return *this;
        }
        ++i;
      }
      pos = MAX;
      return *this;
    }

    const_iterator operator++(int)
    {
      const_iterator tmp(*this);
      ++(*this);
      return tmp;
    }

    bool operator==(const const_iterator& rhs) const
    {
      return pos == rhs.pos;
    }
    bool operator!=(const const_iterator& rhs) const {return !operator==(rhs);}

    const Key &operator*() const { return pos; }
  };
  static_assert(std::input_or_output_iterator<const_iterator>);

private:
  uint64_t words[WORDS];
  const_iterator _end;

public:
  bitset_set() : _end(this, MAX) { clear(); }
  bitset_set(const bitset_set& other) : _end(this, MAX)
  {
    copy(other);
  }
  bitset_set(bitset_set&& other) : bitset_set(other)
  {}
  template< class InputIt >
  bitset_set( InputIt first, InputIt last ) : bitset_set()
  {
    for (InputIt it = first; it != last; ++it) {
      emplace(*it);
    }
  }
  bitset_set(std::initializer_list<Key> init) : bitset_set(init.begin(), init.end())
  {}

  /** insert k into set.  */
  void insert(const Key k) {
    ceph_assert(k < MAX);
    words[k / BITS_PER_UINT64] |= 1UL << (k % BITS_PER_UINT64);
  }

  /** insert k into set.  */
  void insert(const bitset_set other) {
    for (int i = 0; i < WORDS; ++i) {
      words[i] |= other.words[i];
    }
  }

  /* Emplace key. Unusually this is LESS efficient than insert, since the key
   * must be constructed, so the int value can be inserted.  The key is
   * immediately discarded.
   *
   * This also does not return an const_iterator, as this was not needed by any
   * client.
   */
  template< class... Args >
  std::pair<const_iterator, bool> emplace(Args&&... args)
  {
    Key k(args...);
    bool add = !contains(k);
    if (add) insert(k);
    return std::make_pair(const_iterator(this, k), add);
  }

  void erase(const Key k) {
    ceph_assert(k < MAX);
    words[k / BITS_PER_UINT64] &= ~(1UL << (k % BITS_PER_UINT64));
  }

  void insert_range(const Key start, int length)
  {
    int start_word = start / BITS_PER_UINT64;
    int end_word = (start + length) / BITS_PER_UINT64;
    ceph_assert(0 <= end_word && end_word < MAX);

    if (start_word == end_word) {
      words[start_word] |= ((1UL << length) - 1) << (start % BITS_PER_UINT64);
    } else {
      words[start_word] |= -1UL << (start % BITS_PER_UINT64);
      while (++start_word < end_word) {
        words[start_word] = -1UL;
      }
      words[end_word] |= (1UL << ((start + length) % BITS_PER_UINT64)) - 1;
    }
  }

  void erase_range(const Key start, int length)
  {
    int start_word = start / BITS_PER_UINT64;
    int end_word = (start + length) / BITS_PER_UINT64;
    ceph_assert(0 <= end_word && end_word < MAX);

    if (start_word == end_word) {
      words[start_word] &= ~(((1UL << length) - 1) << (start % BITS_PER_UINT64));
    } else {
      words[start_word] &= ~(-1UL << (start % BITS_PER_UINT64));
      while (++start_word < end_word) {
        words[start_word] = 0;
      }
      words[end_word] &= ~((1UL << ((start + length) % BITS_PER_UINT64)) - 1);
    }
  }

  void clear() {
    for (size_t i = 0; i < WORDS; ++i) {
      words[i] = 0;
    }
  }

  bool empty() const {
    bool empty = true;
    for (size_t i = 0; i < WORDS; ++i) {
      if (words[i] != 0) {
        empty = false;
        break;
      }
    }
    return empty;
  }

  bool contains(Key k) const
  {
    ceph_assert(k < MAX);
    return (words[k / BITS_PER_UINT64] & 1UL << (k % BITS_PER_UINT64));
  }

  int count(Key k) const {
    return contains(k) ? 1 : 0;
  }

  const_iterator find( const Key& k ) const
  {
    if (contains(k)) return const_iterator(this, k);
    return end();
  }

  size_t size() const {
    size_t count = 0;
    for (size_t i = 0; i < WORDS; ++i) {
      count += std::popcount(words[i]);
    }
    return count;
  }

  size_t max_size() { return MAX; }

  void encode(ceph::buffer::list &bl) const {
    for (size_t i = 0 ; i < WORDS; ++i) {
      denc_varint(words[i], bl);
    }
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    for (size_t i = 0 ; i < WORDS; ++i) {
      denc_varint(words[i], bl);
    }
  }

  // Not possible to modify the keys on the fly, so only const.
  const_iterator begin() const {
    return const_iterator(this);
  }
  const_iterator cbegin() const { return begin(); }
  const_iterator end() const { return _end; }
  const_iterator cend() const { return _end; }

  void copy(const bitset_set &other)
  {
    for (size_t i = 0; i < WORDS; ++i) {
      words[i] = other.words[i];
    }
  }

  bitset_set &operator=(const bitset_set& other)
  {
    copy(other);
    return *this;
  }

  bitset_set &operator=(bitset_set&& other)
  {
    copy(other);
    return *this;
  }

  void swap(bitset_set &other)
  {
    bitset_set tmp(other);
    for (size_t i = 0 ; i < WORDS; ++i) {
      other.words[i] = words[i];
      words[i] = tmp.words[i];
    }
  }

  bool includes(const bitset_set &other) const
  {
    for (size_t i = 0; i < WORDS; ++i) {
      if ((words[i] & other.words[i]) != other.words[i]) return false;
    }
    return true;
  }

  friend bool operator==( const bitset_set& lhs, const bitset_set& rhs )
  {
    for (size_t i = 0 ; i < WORDS; ++i) {
      if (lhs.words[i] != rhs.words[i]) return false;
    }
    return true;
  }

  friend std::ostream& operator<<(std::ostream& lhs, const bitset_set& rhs)
  {
    int c = 0;
    lhs << "{";
    for (auto &&k : rhs) {
      lhs << k;
      c++;
      if (c < rhs.size()) lhs << ",";
    }
    lhs << "}";
    return lhs;
  }

  static bitset_set difference(const bitset_set& lhs, const bitset_set& rhs)
  {
    bitset_set res;
    for (size_t i = 0 ; i < WORDS; ++i) {
      res.words[i] = lhs.words[i] & ~rhs.words[i];
    }
    return res;
  }

  static bitset_set intersection(const bitset_set& lhs, const bitset_set& rhs)
  {
    bitset_set res;
    for (size_t i = 0 ; i < WORDS; ++i) {
      res.words[i] = lhs.words[i] & rhs.words[i];
    }
    return res;
  }

  friend std::strong_ordering operator<=>(const bitset_set &lhs, const bitset_set &rhs)
  {
    for (size_t i = 0 ; i < WORDS; ++i) {
      if (lhs.words[i] != rhs.words[i]) return lhs.words[i] <=> rhs.words[i];
    }

    return std::strong_ordering::equal;
  }
};
