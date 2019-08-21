# Package

version       = "0.1.0"
author        = "Ivan Florentin"
description   = "Automatic REsT API over PostgreSQL database"
license       = "MIT"
srcDir        = "src"
installExt    = @["nim"]
bin           = @["autopg_tools"]


# Dependencies

requires "nim >= 0.20.9"
requires "https://github.com/synarchys/pgschemautils.git"
requires "https://github.com/ivanflorentin/noah.git"
