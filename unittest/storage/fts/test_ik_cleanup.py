"""Compile the production cleanup body with instrumented resource doubles.

Run with Python 3 and a C++ compiler. This focused fault-path regression does
not replace building seekdb or running TOKENIZE / MATCH integration tests.
"""

import argparse
import pathlib
import subprocess
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[3]
SOURCE = ROOT / "src/storage/fts/ob_ik_ft_parser.cpp"


def definition(source, signature):
    start = source.index(signature)
    opening = source.index("{", start)
    depth = 1
    end = opening + 1
    while depth:
        depth += (source[end] == "{") - (source[end] == "}")
        end += 1
    return source[start:end]


HARNESS = r'''
#include <cassert>
#include <vector>
#define OB_ISNULL(p) ((p) == nullptr)
static int live = 0;
struct TokenizeContext { TokenizeContext() { ++live; } ~TokenizeContext() { --live; } };
struct ObIFTDict { ObIFTDict() { ++live; } virtual ~ObIFTDict() { --live; } };
struct ObIIKProcessor { ObIIKProcessor() { ++live; } virtual ~ObIIKProcessor() { --live; } };
struct Allocator { void free(void *p) { ::operator delete(p); } };
struct Cache { void reset() { assert(live == 0); } };
struct ObIKFTParser {
  Allocator allocator_;
  TokenizeContext *ctx_ = nullptr;
  std::vector<ObIIKProcessor *> segmenters_;
  ObIFTDict *dict_main_ = nullptr;
  ObIFTDict *dict_quan_ = nullptr;
  Cache cache_main_, cache_quan_;
  bool is_inited_ = false;
  void reset();
  ~ObIKFTParser() { reset(); }
};
__RESET__
int main() {
  // Cover every partially acquired context/dictionary combination, with
  // zero or several processors, then cleanup, retry, and destruction.
  for (int mask = 0; mask < 8; ++mask) {
    for (int processors = 0; processors <= 3; ++processors) {
      ObIKFTParser parser;
      for (int retry = 0; retry < 3; ++retry) {
        if (mask & 1) parser.ctx_ = new TokenizeContext;
        if (mask & 2) parser.dict_main_ = new ObIFTDict;
        if (mask & 4) parser.dict_quan_ = new ObIFTDict;
        for (int i = 0; i < processors; ++i)
          parser.segmenters_.push_back(new ObIIKProcessor);
        parser.is_inited_ = true;
        parser.reset();
        assert(live == 0);
        assert(parser.ctx_ == nullptr);
        assert(parser.dict_main_ == nullptr);
        assert(parser.dict_quan_ == nullptr);
        assert(parser.segmenters_.empty());
        assert(!parser.is_inited_);
        parser.reset();
      }
    }
  }
}
'''


class CleanupRegression(unittest.TestCase):
    def test_partial_cleanup_retry_and_destruction(self):
        source = SOURCE.read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as directory:
            cpp = pathlib.Path(directory) / "cleanup.cpp"
            binary = pathlib.Path(directory) / "cleanup"
            cpp.write_text(HARNESS.replace("__RESET__", definition(source, "void ObIKFTParser::reset()")), encoding="utf-8")
            subprocess.run(["g++", "-std=c++17", "-Wall", "-Wextra", "-Werror",
                            "-fsanitize=address,undefined", "-g", str(cpp), "-o", str(binary)], check=True)
            subprocess.run([str(binary)], check=True)

    def test_missing_hub_stops_initialization(self):
        source = SOURCE.read_text(encoding="utf-8")
        harness = r'''
#include <cassert>
constexpr int OB_SUCCESS = 0, OB_ERR_UNEXPECTED = -1;
#define OB_ISNULL(p) ((p) == nullptr)
#define OB_FAIL(expr) ((ret = (expr)) != OB_SUCCESS)
struct ObFTParserParam {};
enum class ObFTDictType { DICT_IK_MAIN, DICT_IK_QUAN };
enum class ObCharsetType { CHARSET_UTF8MB4 };
enum class ObCollationType { CS_TYPE_UTF8MB4_BIN };
struct ObFTDictDesc {
  ObFTDictDesc(const char *, ObFTDictType, ObCharsetType, ObCollationType) {}
};
struct ObIKFTParser {
  void *hub_ = nullptr;
  int cache_main_ = 0, cache_quan_ = 0, dict_main_ = 0, dict_quan_ = 0;
  bool should_read_newest_table() { return false; }
  int init_single_dict(ObFTDictDesc, int &) {
    assert(hub_ != nullptr); return OB_SUCCESS;
  }
  int build_dict_from_cache(ObFTDictDesc, int &, int &) {
    assert(hub_ != nullptr); return OB_SUCCESS;
  }
  int init_dict(const ObFTParserParam &);
};
__INIT__
int main() {
  ObIKFTParser parser;
  assert(parser.init_dict({}) == OB_ERR_UNEXPECTED);
  parser.hub_ = &parser;
  assert(parser.init_dict({}) == OB_SUCCESS);
}
'''
        with tempfile.TemporaryDirectory() as directory:
            cpp = pathlib.Path(directory) / "hub.cpp"
            binary = pathlib.Path(directory) / "hub"
            cpp.write_text(harness.replace("__INIT__", definition(source, "int ObIKFTParser::init_dict(")), encoding="utf-8")
            subprocess.run(["g++", "-std=c++17", "-fsanitize=address,undefined",
                            str(cpp), "-o", str(binary)], check=True)
            subprocess.run([str(binary)], check=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=pathlib.Path,
                        default=ROOT / "src/storage/fts/ob_ik_ft_parser.cpp")
    args, remaining = parser.parse_known_args()
    SOURCE = args.source
    unittest.main(argv=[__file__, *remaining])
