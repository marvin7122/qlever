// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <chrono>
#include <iostream>
#include <memory>
#include <vector>

#include <asmjit/x86.h>

#include "engine/sparqlExpressions/JitExpressionBytecodeVm.h"

using namespace ql::engine::jit;

// Mock virtual AST expression tree (representing existing SparqlExpression)
struct AstNode {
  virtual ~AstNode() = default;
  [[nodiscard]] virtual int64_t evaluate(const int64_t* row) const = 0;
};

struct ColNode : public AstNode {
  size_t colIdx;
  explicit ColNode(size_t idx) : colIdx(idx) {}
  int64_t evaluate(const int64_t* row) const override { return row[colIdx]; }
};

struct ConstNode : public AstNode {
  int64_t val;
  explicit ConstNode(int64_t v) : val(v) {}
  int64_t evaluate(const int64_t*) const override { return val; }
};

struct MulNode : public AstNode {
  std::unique_ptr<AstNode> left, right;
  MulNode(std::unique_ptr<AstNode> l, std::unique_ptr<AstNode> r) : left(std::move(l)), right(std::move(r)) {}
  int64_t evaluate(const int64_t* row) const override { return left->evaluate(row) * right->evaluate(row); }
};

struct AddNode : public AstNode {
  std::unique_ptr<AstNode> left, right;
  AddNode(std::unique_ptr<AstNode> l, std::unique_ptr<AstNode> r) : left(std::move(l)), right(std::move(r)) {}
  int64_t evaluate(const int64_t* row) const override { return left->evaluate(row) + right->evaluate(row); }
};

struct GtNode : public AstNode {
  std::unique_ptr<AstNode> left, right;
  GtNode(std::unique_ptr<AstNode> l, std::unique_ptr<AstNode> r) : left(std::move(l)), right(std::move(r)) {}
  int64_t evaluate(const int64_t* row) const override { return (left->evaluate(row) > right->evaluate(row)) ? 1 : 0; }
};

int main() {
  constexpr size_t NUM_ROWS = 10'000'000;
  std::cout << "=================================================================\n";
  std::cout << "Comparative Benchmark: Virtual AST Tree vs JIT Bytecode VM ("
            << NUM_ROWS << " rows)\n";
  std::cout << "Expression: (col0 * 2) + col1 > 100\n";
  std::cout << "=================================================================\n";

  std::vector<int64_t> col0(NUM_ROWS);
  std::vector<int64_t> col1(NUM_ROWS);
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    col0[i] = static_cast<int64_t>(i % 100);
    col1[i] = static_cast<int64_t>((i * 3) % 100);
  }

  // 1. BASELINE: Virtual AST Tree Interpretation
  auto ast = std::make_unique<GtNode>(
      std::make_unique<AddNode>(
          std::make_unique<MulNode>(std::make_unique<ColNode>(0), std::make_unique<ConstNode>(2)),
          std::make_unique<ColNode>(1)),
      std::make_unique<ConstNode>(100));

  auto b0 = std::chrono::high_resolution_clock::now();
  size_t astMatches = 0;
  int64_t row[2];
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    row[0] = col0[i];
    row[1] = col1[i];
    astMatches += ast->evaluate(row);
  }
  auto b1 = std::chrono::high_resolution_clock::now();
  double astMs = std::chrono::duration<double, std::milli>(b1 - b0).count();

  // 2. PROTOTYPE: JIT Bytecode Program
  JitBytecodeProgram program;
  program.addInstruction(OpCode::LOAD_COL_INT, 0);
  program.addInstruction(OpCode::LOAD_CONST_INT, 2);
  program.addInstruction(OpCode::MUL_INT);
  program.addInstruction(OpCode::LOAD_COL_INT, 1);
  program.addInstruction(OpCode::ADD_INT);
  program.addInstruction(OpCode::LOAD_CONST_INT, 100);
  program.addInstruction(OpCode::CMP_GT_INT);
  program.addInstruction(OpCode::RET);

  auto p0 = std::chrono::high_resolution_clock::now();
  size_t jitMatches = 0;
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    row[0] = col0[i];
    row[1] = col1[i];
    jitMatches += program.execute(row);
  }
  auto p1 = std::chrono::high_resolution_clock::now();
  double jitMs = std::chrono::duration<double, std::milli>(p1 - p0).count();

  std::cout << "\n--- Baseline: Virtual Method AST Tree ---\n";
  std::cout << "Runtime: " << astMs << " ms ("
            << (NUM_ROWS / (astMs / 1000.0)) / 1e6 << " M rows/sec, matches: " << astMatches << ")\n";

  std::cout << "\n--- Prototype: JIT Bytecode VM ---\n";
  std::cout << "Runtime: " << jitMs << " ms ("
            << (NUM_ROWS / (jitMs / 1000.0)) / 1e6 << " M rows/sec, matches: " << jitMatches << ")\n";

  // 3. TIER 2: Native x86-64 Machine-Code JIT via AsmJit
  // Generates native x86-64 loop: (col0 * 2) + col1 > 100
  // Signature: size_t (*)(const int64_t* col0, const int64_t* col1, size_t count)
  using NativeFilterFunc = size_t (*)(const int64_t*, const int64_t*, size_t);

  asmjit::JitRuntime rt;
  asmjit::CodeHolder code;
  code.init(rt.environment());
  asmjit::x86::Compiler cc(&code);

  // Define function signature
  asmjit::FuncNode* func = cc.add_func(asmjit::FuncSignature::build<size_t, const int64_t*, const int64_t*, size_t>());

  asmjit::x86::Gp ptr0 = cc.new_gp_ptr("ptr0");
  asmjit::x86::Gp ptr1 = cc.new_gp_ptr("ptr1");
  asmjit::x86::Gp count = cc.new_gp_ptr("count");
  asmjit::x86::Gp matches = cc.new_gp64("matches");
  asmjit::x86::Gp idx = cc.new_gp_ptr("idx");

  func->set_arg(0, ptr0);
  func->set_arg(1, ptr1);
  func->set_arg(2, count);

  cc.xor_(matches, matches);
  cc.xor_(idx, idx);

  asmjit::Label loopStart = cc.new_label();
  asmjit::Label loopEnd = cc.new_label();

  cc.bind(loopStart);
  cc.cmp(idx, count);
  cc.jge(loopEnd);

  // Load col0[idx] and col1[idx]
  asmjit::x86::Gp v0 = cc.new_gp64("v0");
  asmjit::x86::Gp v1 = cc.new_gp64("v1");
  asmjit::x86::Gp sum = cc.new_gp64("sum");

  cc.mov(v0, asmjit::x86::qword_ptr(ptr0, idx, 3)); // scale 8 = 1 << 3
  cc.mov(v1, asmjit::x86::qword_ptr(ptr1, idx, 3));

  // Compute: (v0 * 2) + v1
  cc.lea(sum, asmjit::x86::qword_ptr(v1, v0, 1)); // sum = v1 + v0 * 2

  // Compare > 100
  cc.cmp(sum, 100);
  asmjit::x86::Gp matchBit = cc.new_gp64("matchBit");
  cc.setg(matchBit.r8());
  cc.movzx(matchBit, matchBit.r8());
  cc.add(matches, matchBit);

  cc.inc(idx);
  cc.jmp(loopStart);

  cc.bind(loopEnd);
  cc.ret(matches);
  cc.end_func();

  cc.finalize();

  auto c0 = std::chrono::high_resolution_clock::now();
  NativeFilterFunc nativeFn = nullptr;
  asmjit::Error err = rt.add(&nativeFn, &code);
  auto c1 = std::chrono::high_resolution_clock::now();
  double compileUs = std::chrono::duration<double, std::micro>(c1 - c0).count();

  if (err != asmjit::Error::kOk || !nativeFn) {
    std::cerr << "AsmJit compilation failed: code=" << static_cast<unsigned int>(err) << "\n";
    return 1;
  }

  // Benchmark Native JIT execution
  auto n0 = std::chrono::high_resolution_clock::now();
  size_t asmjitMatches = nativeFn(col0.data(), col1.data(), NUM_ROWS);
  auto n1 = std::chrono::high_resolution_clock::now();
  double asmMs = std::chrono::duration<double, std::milli>(n1 - n0).count();

  std::cout << "\n--- Tier 2: Native x86-64 Machine-Code JIT (AsmJit) ---\n";
  std::cout << "Compile Time: " << compileUs << " us\n";
  std::cout << "Runtime:      " << asmMs << " ms ("
            << (NUM_ROWS / (asmMs / 1000.0)) / 1e6 << " M rows/sec, matches: " << asmjitMatches << ")\n";

  std::cout << "\n=================================================================\n";
  std::cout << ">>> Bytecode VM vs Virtual AST: " << (astMs / jitMs) << "x\n";
  std::cout << ">>> Native AsmJit vs Virtual AST: " << (astMs / asmMs) << "x faster!\n";
  std::cout << ">>> Native AsmJit vs Bytecode VM:  " << (jitMs / asmMs) << "x faster!\n";
  std::cout << "=================================================================\n";

  return 0;
}
