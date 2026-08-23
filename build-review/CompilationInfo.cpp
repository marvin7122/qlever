#include "CompilationInfo.h"
namespace qlever::version {
constexpr std::string_view GitHash = "b53c7646a7ce19f696700b36eba2a98f5bfadf08"
;
constexpr std::string_view GitShortHash = GitHash.substr(0, 7);
constexpr std::string_view DatetimeOfCompilation = "Thu Jul 30 02:57:51 PM CEST 2026";
constexpr std::string_view TimeOfCompilationUnix = "1785416271";
constexpr std::string_view ProjectVersion = "v0.5.50-55-gb53c7646";
constexpr std::string_view Compiler = "GNU";
constexpr std::string_view CompilerVersion = "15.2.1";
constexpr std::string_view CxxStandard = "20";

void copyVersionInfo() {
  *gitShortHashWithoutLinking.wlock() = GitShortHash;
  *datetimeOfCompilationWithoutLinking.wlock() = DatetimeOfCompilation;
  *timeOfCompilationUnixWithoutLinking.wlock() = TimeOfCompilationUnix;
  *projectVersionWithoutLinking.wlock() = ProjectVersion;
  *compilerWithoutLinking.wlock() = Compiler;
  *compilerVersionWithoutLinking.wlock() = CompilerVersion;
  *cxxStandardWithoutLinking.wlock() = CxxStandard;
}
}