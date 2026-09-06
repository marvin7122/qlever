// Copyright 2024 - 2026, The QLever Authors, in particular:
//
// 2024 - 2026 Johannes Kalmbach <johannes.kalmbach@gmail.com>, UFR
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//

//
// Set author to Johannes Kalmbach.
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_split.h>
#include <gmock/gmock.h>

#include <array>
#include <memory>
#include <string_view>

#include "backports/span.h"
#include "util/FsstCompressor.h"
#include <range/v3/view/zip.hpp>

TEST(FsstEncoder, firstTest) {
  std::vector<std::string> s{
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna "
      "aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo "
      "dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus "
      "est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur "
      "sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et "
      "dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam "
      "et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea "
      "takimata sanctus est Lorem ipsum dolor sit amet. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis at vero "
      "eros et accumsan et iusto odio dignissim qui blandit praesent luptatum "
      "zzril delenit augue duis dolore te feugait nulla facilisi. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. \n"
      "\n"
      "Ut wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. Duis autem "
      "vel eum iriure dolor in hendrerit in vulputate velit esse molestie "
      "consequat, vel illum dolore eu feugiat nulla facilisis at vero eros et "
      "accumsan et iusto odio dignissim qui blandit praesent luptatum zzril "
      "delenit augue duis dolore te feugait nulla facilisi. \n"
      "\n"
      "Nam liber tempor cum soluta nobis eleifend option congue nihil "
      "imperdiet doming id quod mazim placerat facer possim assum. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. Ut "
      "wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis. \n"
      "\n"
      "At vero eos et accusam et justo duo dolores et ea rebum. Stet clita "
      "kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, At accusam aliquyam diam diam dolore dolores duo eirmod eos "
      "erat, et nonumy sed tempor et et invidunt justo labore Stet clita ea et "
      "gubergren, kasd magna no rebum. sanctus sea sed takimata ut vero "
      "voluptua. est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, "
      "consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat. \n"
      "\n"
      "Consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et "
      "accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no "
      "sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor "
      "sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor "
      "invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At "
      "vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd "
      "gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus. Lorem ipsum "
      "dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod "
      "tempor invidunt ut labore et dolore magna aliquyam erat, sed diam "
      "voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet "
      "clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit "
      "amet. Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna "
      "aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo "
      "dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus "
      "est Lorem ipsum dolor sit amet. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis at vero "
      "eros et accumsan et iusto odio dignissim qui blandit praesent luptatum "
      "zzril delenit augue duis dolore te feugait nulla facilisi. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. \n"
      "\n"
      "Ut wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. Duis autem "
      "vel eum iriure dolor in hendrerit in vulputate velit esse molestie "
      "consequat, vel illum dolore eu feugiat nulla facilisis at vero eros et "
      "accumsan et iusto odio dignissim qui blandit praesent luptatum zzril "
      "delenit augue duis dolore te feugait nulla facilisi. \n"
      "\n"
      "Nam liber tempor cum soluta nobis eleifend option congue nihil "
      "imperdiet doming id quod mazim placerat facer possim assum. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. Ut "
      "wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis. \n"
      "\n"
      "At vero eos et accusam et justo duo dolores et ea rebum. Stet clita "
      "kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, At accusam aliquyam diam diam dolore dolores duo eirmod eos "
      "erat, et nonumy sed tempor et et invidunt justo labore Stet clita ea et "
      "gubergren, kasd magna no rebum. sanctus sea sed takimata ut vero "
      "voluptua. est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, "
      "consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat. \n"
      "\n"
      "Consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et "
      "accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no "
      "sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor "
      "sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor "
      "invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At "
      "vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd "
      "gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus. Lorem ipsum "
      "dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod "
      "tempor invidunt ut labore et dolore magna aliquyam erat, sed diam "
      "voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet "
      "clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit "
      "amet. Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna "
      "aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo "
      "dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus "
      "est Lorem ipsum dolor sit amet. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis at vero "
      "eros et accumsan et iusto odio dignissim qui blandit praesent luptatum "
      "zzril delenit augue duis dolore te feugait nulla facilisi. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. \n"
      "\n"
      "Ut wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. Duis autem "
      "vel eum iriure dolor in hendrerit in vulputate velit esse molestie "
      "consequat, vel illum dolore eu feugiat nulla facilisis at vero eros et "
      "accumsan et iusto odio dignissim qui blandit praesent luptatum zzril "
      "delenit augue duis dolore te feugait nulla facilisi. \n"
      "\n"
      "Nam liber tempor cum soluta nobis eleifend option congue nihil "
      "imperdiet doming id quod mazim placerat facer possim assum. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. Ut "
      "wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis. \n"
      "\n"
      "At vero eos et accusam et justo duo dolores et ea rebum. Stet clita "
      "kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, At accusam aliquyam diam diam dolore dolores duo eirmod eos "
      "erat, et nonumy sed tempor et et invidunt justo labore Stet clita ea et "
      "gubergren, kasd magna no rebum. sanctus sea sed takimata ut vero "
      "voluptua. est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, "
      "consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat. \n"
      "\n"
      "Consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et "
      "accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no "
      "sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor "
      "sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor "
      "invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At "
      "vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd "
      "gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus. Lorem ipsum "
      "dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod "
      "tempor invidunt ut labore et dolore magna aliquyam erat, sed diam "
      "voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet "
      "clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit "
      "amet. Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna "
      "aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo "
      "dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus "
      "est Lorem ipsum dolor sit amet. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis at vero "
      "eros et accumsan et iusto odio dignissim qui blandit praesent luptatum "
      "zzril delenit augue duis dolore te feugait nulla facilisi. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. \n"
      "\n"
      "Ut wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. Duis autem "
      "vel eum iriure dolor in hendrerit in vulputate velit esse molestie "
      "consequat, vel illum dolore eu feugiat nulla facilisis at vero eros et "
      "accumsan et iusto odio dignissim qui blandit praesent luptatum zzril "
      "delenit augue duis dolore te feugait nulla facilisi. \n"
      "\n"
      "Nam liber tempor cum soluta nobis eleifend option congue nihil "
      "imperdiet doming id quod mazim placerat facer possim assum. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. Ut "
      "wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis. \n"
      "\n"
      "At vero eos et accusam et justo duo dolores et ea rebum. Stet clita "
      "kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, At accusam aliquyam diam diam dolore dolores duo eirmod eos "
      "erat, et nonumy sed tempor et et invidunt justo labore Stet clita ea et "
      "gubergren, kasd magna no rebum. sanctus sea sed takimata ut vero "
      "voluptua. est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, "
      "consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat. \n"
      "\n"
      "Consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et "
      "accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no "
      "sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor "
      "sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor "
      "invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At "
      "vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd "
      "gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus. Lorem ipsum "
      "dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod "
      "tempor invidunt ut labore et dolore magna aliquyam erat, sed diam "
      "voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet "
      "clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit "
      "amet. Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna "
      "aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo "
      "dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus "
      "est Lorem ipsum dolor sit amet. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis at vero "
      "eros et accumsan et iusto odio dignissim qui blandit praesent luptatum "
      "zzril delenit augue duis dolore te feugait nulla facilisi. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. \n"
      "\n"
      "Ut wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. Duis autem "
      "vel eum iriure dolor in hendrerit in vulputate velit esse molestie "
      "consequat, vel illum dolore eu feugiat nulla facilisis at vero eros et "
      "accumsan et iusto odio dignissim qui blandit praesent luptatum zzril "
      "delenit augue duis dolore te feugait nulla facilisi. \n"
      "\n"
      "Nam liber tempor cum soluta nobis eleifend option congue nihil "
      "imperdiet doming id quod mazim placerat facer possim assum. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. Ut "
      "wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis. \n"
      "\n"
      "At vero eos et accusam et justo duo dolores et ea rebum. Stet clita "
      "kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, At accusam aliquyam diam diam dolore dolores duo eirmod eos "
      "erat, et nonumy sed tempor et et invidunt justo labore Stet clita ea et "
      "gubergren, kasd magna no rebum. sanctus sea sed takimata ut vero "
      "voluptua. est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, "
      "consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat. \n"
      "\n"
      "Consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et "
      "accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no "
      "sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor "
      "sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor "
      "invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At "
      "vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd "
      "gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus. Lorem ipsum "
      "dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod "
      "tempor invidunt ut labore et dolore magna aliquyam erat, sed diam "
      "voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet "
      "clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor "};
  s = absl::StrSplit(s.front(), " ");

  // First test the manual interface.
  FsstEncoder encoder{s};
  std::vector<std::string> s2;
  size_t original = 0;
  size_t compressed = 0;
  for (auto& str : s) {
    s2.push_back(encoder.compress(str));
    original += str.size();
    compressed += s2.back().size();
  }
  EXPECT_LT(compressed, original);

  auto decoder = encoder.makeDecoder();
  for (auto& str : s2) {
    str = decoder.decompress(str);
  }
  EXPECT_THAT(s2, ::testing::ElementsAreArray(s));

  // Now test the `compressAll` interface.
  {
    auto [buffer, compressedViews, decoder] = FsstEncoder::compressAll(s);
    std::vector<std::string> s3;
    for (auto compressedView : compressedViews) {
      s3.push_back(decoder.decompress(compressedView));
    }
    EXPECT_THAT(s3, ::testing::ElementsAreArray(s));
  }
}

// _____________________________________________________________________________
// Goal: `decompressInto` (the arena-bound path used by `lookupBatch`) must
// produce byte-for-byte the same output as the string-returning `decompress`.
// Method: compress the words, decode each compressed word through both
// interfaces, and compare the results against each other and the original.
TEST(FsstEncoder, DecompressIntoMatchesDecompress) {
  const std::vector<std::string> words{"alpha", "", "beta", "", "gamma"};
  auto [buffer, compressedViews, decoder] = FsstEncoder::compressAll(words);
  for (const auto& [word, compressed] :
       ::ranges::views::zip(words, compressedViews)) {
    const std::string viaString = decoder.decompress(compressed);
    std::string output(decoder.maxDecompressedSize(compressed), '\0');
    const size_t size = decoder.decompressInto(
        compressed, ql::span<char>{output.data(), output.size()});
    const std::string_view decompressedView{output.data(), size};

    EXPECT_EQ(decompressedView, viaString);
    EXPECT_THAT(viaString, ::testing::Eq(word));
  }
}

// _____________________________________________________________________________
// Goal: test FsstDecoder::maxDecompressedSize and decompressInto boundary
// conditions, overflow, buffer sizing, and empty input.
TEST(FsstDecoder, MaxDecompressedSizeAndDecompressInto) {
  const std::vector<std::string> words{"hello", "world", "", "x"};
  auto [buffer, compressedViews, decoder] = FsstEncoder::compressAll(words);
  std::vector<std::string_view> compressed;
  compressed.assign(compressedViews.begin(), compressedViews.end());

  // Test maxDecompressedSize for each compressed word
  for (const auto& cv : compressed) {
    size_t maxSize = decoder.maxDecompressedSize(cv);
    EXPECT_GT(maxSize, 0u);
    // Verify it's at least the decompressed size
    std::string viaString = decoder.decompress(cv);
    EXPECT_LE(viaString.size(), maxSize);
  }

  // Empty input: maxDecompressedSize should handle empty string_view
  size_t emptyMax = decoder.maxDecompressedSize("");
  EXPECT_GE(emptyMax, 0u);

  // Large input overflow boundary: compress a very large string
  const std::string large(10000, 'a');
  auto [buf2, views2, dec2] = FsstEncoder::compressAll({large});
  size_t largeMax = dec2.maxDecompressedSize(views2[0]);
  EXPECT_GT(largeMax, 0u);
  EXPECT_GE(largeMax, large.size());

  // Test decompressInto with exact-sized buffer
  for (const auto& cv : compressed) {
    size_t maxSize = decoder.maxDecompressedSize(cv);
    std::string intoBuf(maxSize, '\0');
    size_t n = decoder.decompressInto(cv, ql::span<char>{intoBuf.data(), intoBuf.size()});
    std::string viaString = decoder.decompress(cv);
    EXPECT_EQ(n, viaString.size());
    EXPECT_EQ(std::string_view(intoBuf.data(), n), viaString);
  }

  // Test decompressInto with larger-than-needed buffer
  for (const auto& cv : compressed) {
    size_t maxSize = decoder.maxDecompressedSize(cv);
    std::string intoBuf(maxSize + 100, '\0');
    size_t n = decoder.decompressInto(cv, ql::span<char>{intoBuf.data(), intoBuf.size()});
    std::string viaString = decoder.decompress(cv);
    EXPECT_EQ(n, viaString.size());
    EXPECT_EQ(std::string_view(intoBuf.data(), n), viaString);
  }

  // Test decompressInto with empty input
  {
    std::string intoBuf(10, '\0');
    size_t n = decoder.decompressInto("", ql::span<char>{intoBuf.data(), intoBuf.size()});
    EXPECT_EQ(n, 0u);
  }
}

// _____________________________________________________________________________
class FsstRepeatedDecoderTest : public ::testing::Test {
 protected:
  // ___________________________________________________________________________
  template <size_t N>
  static void expectRepeatedDecompressIntoMatches(
      const std::vector<std::string>& words) {
    std::vector<std::string_view> compressed;
    compressed.reserve(words.size());
    for (const auto& w : words) {
      compressed.emplace_back(w);
    }

    std::array<FsstDecoder, N> decoders{};
    // Each stage's decoder holds views into that stage's symbol-table buffer,
    // so every buffer must stay alive for as long as `decoders` is used.
    std::vector<std::shared_ptr<std::string>> buffers;
    buffers.reserve(N);
    for (size_t stage = 0; stage < N; ++stage) {
      auto [buffer, nextViews, decoder] = FsstEncoder::compressAll(compressed);
      compressed.assign(nextViews.begin(), nextViews.end());
      decoders[stage] = std::move(decoder);
      buffers.push_back(std::move(buffer));
    }

    FsstRepeatedDecoder<N> repeated{std::move(decoders)};
    std::string scratch;
    for (size_t i = 0; i < words.size(); ++i) {
      const std::string viaString = repeated.decompress(compressed[i]);
      std::string intoBuf(repeated.maxDecompressedSize(compressed[i]), '\0');
      const size_t n = repeated.decompressInto(
          compressed[i], ql::span<char>{intoBuf.data(), intoBuf.size()},
          scratch);
      EXPECT_THAT(n, ::testing::Eq(viaString.size()));
      EXPECT_THAT(std::string_view(intoBuf.data(), n),
                  ::testing::Eq(viaString));
      EXPECT_THAT(viaString, ::testing::Eq(words[i]));

      // Also verify the 2-argument overload without scratch parameter:
      std::string intoBuf2(repeated.maxDecompressedSize(compressed[i]), '\0');
      const size_t n2 = repeated.decompressInto(
          compressed[i], ql::span<char>{intoBuf2.data(), intoBuf2.size()});
      EXPECT_THAT(n2, ::testing::Eq(viaString.size()));
      EXPECT_THAT(std::string_view(intoBuf2.data(), n2),
                  ::testing::Eq(viaString));
    }

    if constexpr (N >= 2) {
      EXPECT_GE(scratch.size(),
                repeated.maxDecompressedSize(compressed.front()));
    }
  }
};

// _____________________________________________________________________________
// Goal: the repeated-decoder variant (FSST stages applied N times) must also
// satisfy `decompressInto` == `decompress` byte-for-byte. Method: shared
// helper `expectRepeatedDecompressIntoMatches<N>` compresses words through N
// cascaded stages, decodes via both interfaces, and compares all three.
TEST_F(FsstRepeatedDecoderTest, decompressIntoMatchesDecompressOneStage) {
  expectRepeatedDecompressIntoMatches<1>(
      {"alpha", "", "beta", "gamma-gamma-gamma", ""});
}

// _____________________________________________________________________________
// Verify decompressInto == decompress byte-for-byte over two cascaded FSST stages.
TEST_F(FsstRepeatedDecoderTest, decompressIntoMatchesDecompressTwoStages) {
  expectRepeatedDecompressIntoMatches<2>(
      {"alpha", "", "beta", "gamma-gamma-gamma", ""});
}

// _____________________________________________________________________________
// Verify decompressInto == decompress byte-for-byte over three cascaded FSST stages.
TEST(FsstEncoder, RepeatedDecoderMaxDecompressedSizeAndDecompressInto) {
  // Test various N, overflow, and both decompressInto overloads
  const std::vector<std::string> words{"hello", "world", ""};
  auto [buffer, compressedViews, decoder] = FsstEncoder::compressAll(words);
  std::vector<std::string_view> compressed;
  compressed.assign(compressedViews.begin(), compressedViews.end());

  // Single stage repeated decoder (N=1)
  FsstRepeatedDecoder<1> repeated1{std::move(decoder)};
  for (const auto& cv : compressed) {
    size_t maxSize = repeated1.maxDecompressedSize(cv);
    // Ensure maxSize is reasonable (not zero for non-empty?)
    std::string intoBuf(maxSize, '\0');
    size_t n = repeated1.decompressInto(cv, ql::span<char>{intoBuf.data(), intoBuf.size()});
    EXPECT_LE(n, maxSize);
    std::string viaString = repeated1.decompress(cv);
    EXPECT_EQ(std::string_view(intoBuf.data(), n), viaString);
  }

  // Test with scratch overload
  for (const auto& cv : compressed) {
    std::string scratch;
    size_t maxSize = repeated1.maxDecompressedSize(cv);
    std::string intoBuf(maxSize, '\0');
    size_t n = repeated1.decompressInto(cv, ql::span<char>{intoBuf.data(), intoBuf.size()}, scratch);
    EXPECT_LE(n, maxSize);
    std::string viaString = repeated1.decompress(cv);
    EXPECT_EQ(std::string_view(intoBuf.data(), n), viaString);
  }

  // Overflow test: ensure maxDecompressedSize does not overflow for large input
  const std::string large(1000, 'x');
  auto [buf2, views2, dec2] = FsstEncoder::compressAll({large});
  FsstRepeatedDecoder<1> rep2{std::move(dec2)};
  size_t maxSize2 = rep2.maxDecompressedSize(views2[0]);
  EXPECT_GT(maxSize2, 0u);
  // Check that decompressInto works
  std::string into2(maxSize2, '\0');
  size_t n2 = rep2.decompressInto(views2[0], ql::span<char>{into2.data(), into2.size()});
  EXPECT_EQ(n2, large.size());
  EXPECT_EQ(std::string_view(into2.data(), n2), large);
}

TEST_F(FsstRepeatedDecoderTest, decompressIntoMatchesDecompressThreeStages) {
  expectRepeatedDecompressIntoMatches<3>(
      {"alpha", "", "beta", "gamma-gamma-gamma", ""});
}
