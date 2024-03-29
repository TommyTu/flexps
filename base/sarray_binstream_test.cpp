#include "glog/logging.h"
#include "gtest/gtest.h"

#include "base/sarray_binstream.hpp"

namespace flexps {
namespace {

class TestSArrayBinStream : public testing::Test {
 public:
  TestSArrayBinStream() {}
  ~TestSArrayBinStream() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

TEST_F(TestSArrayBinStream, ConstructAndSize) {
  SArrayBinStream bin;
  ASSERT_EQ(bin.Size(), 0);
}

TEST_F(TestSArrayBinStream, FromMsg) {
  SArrayBinStream bin;
  Message msg;
  third_party::SArray<int> s{1, 2};
  msg.AddData(s);
  bin.FromMsg(msg);
  ASSERT_EQ(bin.Size(), 8);
  int a, b;
  bin >> a >> b;
  ASSERT_EQ(bin.Size(), 0);
  EXPECT_EQ(a, 1);
  EXPECT_EQ(b, 2);
}

TEST_F(TestSArrayBinStream, ToMsg) {
  SArrayBinStream bin;
  int a = 1;
  int b = 2;
  bin << a << b;
  Message msg = bin.ToMsg();
  ASSERT_EQ(msg.data.size(), 1);
  third_party::SArray<int> s = third_party::SArray<int>(msg.data[0]);
  ASSERT_EQ(s.size(), 2);
  EXPECT_EQ(s[0], a);
  EXPECT_EQ(s[1], b);
}

TEST_F(TestSArrayBinStream, Int) {
  SArrayBinStream bin;
  int a = 10;
  int b = 20;
  bin << a << b;
  ASSERT_EQ(bin.Size(), 8);
  int c, d;
  bin >> c >> d;
  EXPECT_EQ(c, 10);
  EXPECT_EQ(d, 20);
  ASSERT_EQ(bin.Size(), 0);
}

// TODO:  Provide vector overload to enable this
// TEST_F(TestSArrayBinStream, Vector) {
//   SArrayBinStream bin;
//   std::vector<int> v;
//   bin << v;
// }

}  // namespace
}  // namespace flexps
