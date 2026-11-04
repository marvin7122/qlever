// Copyright 2024, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include <absl/strings/str_split.h>
#include <gmock/gmock.h>

#include "util/FsstCompressor.h"

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

// `decompressWithScratch` of `FsstDecoder` and `FsstRepeatedDecoder` yields
// the same words as `decompress`, also when the scratch buffers are reused for
// words of very different sizes (long, then short, then empty, then long
// again).
TEST(FsstDecoder, decompressWithScratchMatchesDecompress) {
  std::vector<std::string> words{std::string(1000, 'a') + "bcdefg", "x", "",
                                 "the quick brown fox jumps over the lazy dog",
                                 std::string(2000, 'z')};
  auto [singleStageBuffer, singleStageViews, singleStageDecoder] =
      FsstEncoder::compressAll(words);
  // The second encoder compresses the output of the first one, so
  // `FsstRepeatedDecoder` has to decode in reverse order (second decoder
  // first).
  auto [twoStageBuffer, twoStageViews, twoStageDecoder] =
      FsstEncoder::compressAll(singleStageViews);
  FsstRepeatedDecoder<2> repeatedDecoder{
      std::array{singleStageDecoder, twoStageDecoder}};

  FsstDecompressionScratch scratch;
  ASSERT_EQ(singleStageViews.size(), words.size());
  for (auto [compressed, word] :
       ::ranges::views::zip(singleStageViews, words)) {
    EXPECT_EQ(singleStageDecoder.decompress(compressed), word);
    EXPECT_EQ(singleStageDecoder.decompressWithScratch(compressed, scratch),
              word);
  }
  ASSERT_EQ(twoStageViews.size(), words.size());
  for (auto [compressed, word] : ::ranges::views::zip(twoStageViews, words)) {
    EXPECT_EQ(repeatedDecoder.decompress(compressed), word);
    EXPECT_EQ(repeatedDecoder.decompressWithScratch(compressed, scratch), word);
  }
}

// `decompressIntoBuffer` grows the buffer to the worst-case size of its input
// and never shrinks it, and a shorter word decompressed into the grown buffer
// is still correct.
TEST(FsstDecoder, decompressIntoBufferGrowsAndReusesTheBuffer) {
  std::vector<std::string> words{std::string(3000, 'q') + "end", "ab"};
  auto [buffer, views, decoder] = FsstEncoder::compressAll(words);
  ASSERT_EQ(views.size(), words.size());
  std::string target;
  EXPECT_EQ(decoder.decompressIntoBuffer(views[0], target), words[0]);
  const size_t grownSize = target.size();
  EXPECT_GE(grownSize, 8 * views[0].size());
  EXPECT_EQ(decoder.decompressIntoBuffer(views[1], target), words[1]);
  EXPECT_EQ(target.size(), grownSize);
}
