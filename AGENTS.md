# AGENTS.md - C11 CMake Project Guidelines

## Project Overview
- **Language**: C11
- **Build System**: CMake (3.15+)
- **Outputs**: Shared library (.so/.dll/.dylib) and Static library (.a/.lib)
- **Standard**: ISO C11 with POSIX extensions where needed

## Build Commands

### Full Build
```bash
mkdir -p build && cd build
cmake ..
make -j$(nproc)
```

### Clean Build
```bash
rm -rf build && mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)
```

### Debug Build
```bash
cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_FLAGS="-g -O0" ..
make -j$(nproc)
```

## Test Commands

### Run All Tests
```bash
cd build && ctest --output-on-failure
```

### Run Single Test
```bash
cd build && ctest -R <test_name> --output-on-failure
```

### Run Specific Test Executable Directly
```bash
cd build && ./tests/test_slab
```

### Run with Valgrind (if available)
```bash
cd build && ctest -T memcheck
```

### Test Files Location
- Tests are in `tests/` directory
- Test files follow pattern: `test_*.c`
- Each test file creates a standalone executable
- Current tests:
  - `tests/test_slab.c` - Slab allocator tests (intrusive queues, dual slabs)
  - `tests/test_engine.c` - Matching engine tests (orderbook, products)

## Code Style Guidelines

### Formatting
- **Indent**: 4 spaces (no tabs)
- **Line Length**: 100 characters max
- **Braces**: K&R style - opening brace on same line
- Use `clang-format` with project `.clang-format` file

### Naming Conventions
- **Files**: lowercase_with_underscores.c/h
- **Types**: PascalCase (e.g., `OpenMatchContext`)
- **Functions**: snake_case with module prefix (e.g., `openmatch_init()`)
- **Macros/Constants**: UPPER_SNAKE_CASE with prefix (e.g., `OM_MAX_BUFFER`)
- **Variables**: snake_case
- **Private/internal**: prefix with underscore (e.g., `_internal_helper()`)

### Includes
- Order: system headers → library headers → local headers
- Use angle brackets for external headers: `#include <stdio.h>`
- Use quotes for local headers: `#include "om_core.h"`
- One header per line
- Alphabetize within groups
- Guard all headers with `#ifndef OM_FILENAME_H`

### Types
- Use `<stdint.h>` types: `uint32_t`, `int64_t`, `size_t`
- Avoid bare `int` for sizes, use `size_t`
- Use `bool` from `<stdbool.h>`
- Explicitly mark `const` for read-only parameters

### Error Handling
- Use return codes (0 = success, negative = error)
- Define error codes in `om_error.h`
- Set `errno` pattern: function returns bool, takes `int *err` param
- Check all malloc/calloc return values
- Use `goto cleanup` pattern for resource cleanup

### Memory Management
- All allocations must have corresponding frees
- Use `calloc()` for zero-initialization
- Never cast malloc return in C
- Document ownership in function documentation

### Documentation
- Use Doxygen-style comments: `/** ... */`
- Document all public API functions
- Mark @param, @return, @note
- Include usage example for complex functions

### Safety
- Check bounds on all array accesses
- Validate all pointer parameters (non-NULL)
- Use `static_assert` for compile-time checks
- Prefer `snprintf` over `sprintf`
- Use `memcpy_s` or bounds-checked alternatives where available

## Lint/Analysis Commands

```bash
# Static analysis with clang-tidy
clang-tidy src/*.c -- -Iinclude

# Format check
cd build && cmake --build . --target format-check

# Apply formatting
cd build && cmake --build . --target format

# Static analysis with cppcheck
cppcheck --enable=all --suppress=missingIncludeSystem src/
```

## Project Structure
```
.
├── AGENTS.md              # This file - coding guidelines for AI agents
├── CMakeLists.txt
├── include/
│   ├── khash.h           # klib hash map (single header)
│   └── openmatch/
│       ├── om_slab.h     # Slab allocator public API
│       ├── om_hash.h     # Hashmap interface
│       └── om_engine.h   # Matching engine API
├── src/
│   ├── CMakeLists.txt
│   ├── om_slab.c         # Slab allocator implementation
│   ├── om_hash_khash.c   # khash backend
│   ├── om_hash_f14.c     # F14 backend (opt-in)
│   └── om_engine.c       # Matching engine implementation
├── tests/
│   ├── CMakeLists.txt
│   ├── test_slab.c       # Slab allocator unit tests
│   └── test_engine.c     # Matching engine tests
└── .clang-format
```
