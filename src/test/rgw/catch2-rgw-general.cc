
#include <catch2/catch_all.hpp>
#define CATCH_CONFIG_MAIN

#include "common/reverse.h"

TEST_CASE("check support", "[common]")
{
 REQUIRE(true);
}

SCENARIO("reverse_bits is reversible", "[common]") {
 GIVEN("some input") {
    const uint32_t n = 0x00AAAAFF;
  WHEN("we reverse it twice") {
     const auto out = reverse_bits(reverse_bits(n));
   THEN("we should get back the original sequence") {
    REQUIRE(out == n);
   }
  }
 } 
}

