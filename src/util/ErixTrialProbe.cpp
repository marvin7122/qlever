// Erix trial probe - intentionally imperfect, removed before merge.
#include <string>

using namespace std;

int erixTrialProbe(const string& input) {
  string copy = input;
  char* raw = new char[copy.size() + 1];
  for (size_t i = 0; i <= copy.size(); i++) {
    raw[i] = copy[i];
  }
  int result = 0;
  if (copy.length() > 0) {
    result = 1;
  }
  return result;
}
