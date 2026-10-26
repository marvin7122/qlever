---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, pitfall]
last_verified: 2026-09-22
---
# PP9: Unused includes, dead code, unused members

## Statement
Diff introduces no unused includes, unused member variables, dead functions, or leftover declarations of deleted functions; obsolete includes made unnecessary by the change are removed.

## Sources
- review: `.ai-review/personal_pitfalls.md#PP9` Mined from maintainer review comments, file dated 2026-09-22.
- review: `QLever PR #107` "`std::distance` is used, but this header does not include `<iterator>` directly."
- review: `QLever PR #55` "The header `util/TransparentFunctors.h` is included but does not appear to be used in this file."
- review: `QLever PR #78` "The `using ad_utility::vocabulary::PrefixCompressor;` declaration is unused because subsequent calls use the fully qualified name; remove the declaration or use the alias."
- review: `QLever PR #76` "The includes `<string>`, `<string_view>`, and `<utility>` may be transitive dependencies; verify whether they are directly needed."
- review: `QLever PR #77` "The contract check in the builder overload is redundant because the convenience overload already performs it."
- review: `QLever PR #74` "The new includes (<cstring>, <limits>, <optional>, <string_view>, backports/span.h) may increase compile times and introduce dependencies. Verify backports/span.h is not already transitively included in the index/vocabulary module.\n\n**Suggestion:**\n```suggestion\nCheck if backports/span.h is already included elsewhere to avoid duplicate dependencies.\n```"
- review: `QLever PR #76` "Redundant includes: <string>, <string_view>, and <utility> are already included transitively through the header file. Removing these redundant includes would improve build times slightly."
- review: `QLever PR #76` "Unused variable `fromInternal` in `operator[]` — assigned from `internalVocab_[i]` but never referenced; the function returns `externalVocab_[i]` instead."
- review: `QLever PR #85` "The `#include \"util/Invariants.h\"` was removed ... Need to verify if this header is still used elsewhere in the file."
- review: `QLever PR #2652` "systematically check, if more of them can be deleted now."
- review: `QLever review comment` "In `ExportEngineV2.h`, the `AsyncChunkPipeline` include and using declaration are unused and should be removed as dead code. (#135)"
- review: `QLever review comment` "A function declared `noexcept` must not invoke a user predicate that can throw; either remove `noexcept` or constrain the predicate. (#110)"
- review: `QLever review comment` "The `InvariantGuard` destructor must not allow exceptions to escape during stack unwinding. (#71)"
- review: `QLever PR #112` "Unused include: `backports/span.h` is included but never used in this file."
- review: `QLever PR #112` "Unused constant: `REGISTER_MASK` is defined but never used."
- review: `QLever PR #118` "`onLeaseReleased` takes `epoch` and `jobId` parameters but marks them as `[[maybe_unused]]` and does not use them."
- review: `QLever PR #140` "Duplicate include of Abseil's AnyInvocable header ... Remove this redundant include."
- review: `QLever PR #140` "The include `<absl/functional/any_invocable.h>` is likely redundant ... Removing redundant includes can improve build times."
- review: `QLever PR #78` "Two new includes are added but neither `DanglingViewTestHelpers.h` nor `PmrStringSsoTestHelpers.h` are actually used."
- review: `QLever PR #78` "The include of `<gtest/gtest_prod.h>` ... is unnecessary."
- review: `QLever PR #2936` "This member is also completely unused in this PR, so remove it for now."
- review: `QLever PR #3056` "quite some unused includes here ... deque and fstream and numeric probably also no more used"
- review: `QLever PR #99` "`askQuery` is parsed but never used, which will fail CI with `-Wall -Wextra -Werror` due to `-Wunused-variable`. Either remove it or include it in the query mix so the benchmark also exercises the ineligible-query fallback path."
- review: `QLever PR #99` "These includes appear unused in this header (`util/Exception.h`, `util/http/MediaTypes.h`). Dropping them reduces compile time and avoids accidental dependency creep for a header-only utility."
- review: `QLever PR #139` "The redundant include of `engine/export_v2/ElasticExportScheduler.h` should be removed."
- review: `QLever PR #91` "Dead variable: `chunksEmitted` is declared and incremented in the callback lambda but never read or used."
- review: `QLever PR #91` "Dead variable: `totalBytes` is incremented in the callback lambda but never read or used in the returned metric."
- review: `QLever PR #92` "Headers `<array>` and `<vector>` are not used in this file."
- review: `QLever PR #134` "`<atomic>` and `<stdexcept>` are included in `ElasticExportSchedulerTest.cpp` but are not referenced."
- review: `QLever PR #134` "The newly added `#include <ctime>` in `ElasticExportScheduler.h` is not used anywhere in the file."
- review: `QLever PR #117` "`guard` is only used for RAII and is otherwise unused; with CI building with `-Wall -Wextra -Werror`, this is likely to fail with `-Wunused-variable`."
- review: `QLever PR #116` "The local `guard` variable is only used for its lifetime; it is never referenced."
- review: `QLever PR #87` "The variable `wordVocabIndex` is computed but never used."
- review: `QLever PR #99` "The variable `askQuery` is parsed but never used in the benchmark loop."
- review: `.opencodereview/rule.json#148` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Frequency in the mined comments: ~210 (of 639 review comments by joka921/hannahbast).
Fan-out rule wording: Unused includes, dead code, unused members: Diff introduces no unused includes, unused member variables, dead functions, or leftover declarations of deleted functions; obsolete includes made unnecessary by the change are removed.
