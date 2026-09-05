// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include "engine/sparqlExpressions/JitExpressionCompiler.h"

#include <asmjit/core.h>
#include <asmjit/x86.h>

#include "engine/sparqlExpressions/JitExpressionBytecodeVm.h"
#include "engine/sparqlExpressions/SparqlExpression.h"

namespace ql::engine::jit {

// _____________________________________________________________________________
std::optional<JitCompiledExpression> JitExpressionCompiler::compile(
    const sparqlExpression::SparqlExpression& expr,
    const VariableToColumnMap& varColMap) {
  // First lower the AST into our verified linearized bytecode representation
  auto optProgram = JitExpressionBytecodeVm::compile(expr, varColMap);
  if (!optProgram.has_value()) {
    return std::nullopt;
  }

  const auto& program = optProgram.value();
  const auto& instructions = program.instructions();
  const auto& referencedCols = program.referencedColumns();

  if (instructions.empty()) {
    return std::nullopt;
  }

  auto rt = std::make_shared<asmjit::JitRuntime>();
  asmjit::CodeHolder code;
  code.init(rt->environment());
  asmjit::x86::Compiler cc(&code);

  // Signature:
  // size_t func(const uint64_t* const* colPtrs, size_t count, uint64_t*
  // outMask)
  asmjit::FuncNode* func =
      cc.add_func(asmjit::FuncSignature::build<size_t, const uint64_t* const*,
                                               size_t, uint64_t*>());

  asmjit::x86::Gp colPtrsReg = cc.new_gp_ptr("colPtrs");
  asmjit::x86::Gp countReg = cc.new_gp_ptr("count");
  asmjit::x86::Gp outMaskPtr = cc.new_gp_ptr("outMask");

  func->set_arg(0, colPtrsReg);
  func->set_arg(1, countReg);
  func->set_arg(2, outMaskPtr);

  asmjit::x86::Gp totalMatches = cc.new_gp64("totalMatches");
  asmjit::x86::Gp matchMask = cc.new_gp64("matchMask");
  asmjit::x86::Gp idx = cc.new_gp_ptr("idx");

  cc.xor_(totalMatches, totalMatches);
  cc.xor_(matchMask, matchMask);
  cc.xor_(idx, idx);

  // Load individual column data pointers: colData[i] = colPtrs[i]
  std::vector<asmjit::x86::Gp> colBaseRegs;
  colBaseRegs.reserve(referencedCols.size());
  for (size_t i = 0; i < referencedCols.size(); ++i) {
    asmjit::x86::Gp p = cc.new_gp_ptr();
    cc.mov(p, asmjit::x86::qword_ptr(colPtrsReg, i * 8));
    colBaseRegs.push_back(p);
  }

  asmjit::Label loopStart = cc.new_label();
  asmjit::Label loopEnd = cc.new_label();

  cc.bind(loopStart);
  cc.cmp(idx, countReg);
  cc.jge(loopEnd);

  // Simulated virtual stack for registers during bytecode lowering
  std::vector<asmjit::x86::Gp> regStack;

  for (const auto& inst : instructions) {
    switch (inst.op) {
      case OpCode::LOAD_COL_INT: {
        // Find which index in referencedCols corresponds to inst.arg
        size_t colIdx = 0;
        for (size_t i = 0; i < referencedCols.size(); ++i) {
          if (referencedCols[i] == static_cast<ColumnIndex>(inst.arg)) {
            colIdx = i;
            break;
          }
        }
        asmjit::x86::Gp rawVal = cc.new_gp64("rawVal");
        cc.mov(rawVal, asmjit::x86::qword_ptr(colBaseRegs[colIdx], idx, 3));

        // ValueId integer unpacking: 4 datatype bits, 60 value bits
        // Arithmetic shift right by 4 after shift left by 4 to sign-extend
        cc.shl(rawVal, 4);
        cc.sar(rawVal, 4);
        regStack.push_back(rawVal);
        break;
      }
      case OpCode::LOAD_CONST_INT: {
        asmjit::x86::Gp c = cc.new_gp64("constVal");
        cc.mov(c, inst.arg);
        regStack.push_back(c);
        break;
      }
      case OpCode::ADD_INT: {
        if (regStack.size() < 2) return std::nullopt;
        asmjit::x86::Gp b = regStack.back();
        regStack.pop_back();
        asmjit::x86::Gp a = regStack.back();
        regStack.pop_back();
        asmjit::x86::Gp res = cc.new_gp64("addRes");
        cc.mov(res, a);
        cc.add(res, b);
        regStack.push_back(res);
        break;
      }
      case OpCode::SUB_INT: {
        if (regStack.size() < 2) return std::nullopt;
        asmjit::x86::Gp b = regStack.back();
        regStack.pop_back();
        asmjit::x86::Gp a = regStack.back();
        regStack.pop_back();
        asmjit::x86::Gp res = cc.new_gp64("subRes");
        cc.mov(res, a);
        cc.sub(res, b);
        regStack.push_back(res);
        break;
      }
      case OpCode::MUL_INT: {
        if (regStack.size() < 2) return std::nullopt;
        asmjit::x86::Gp b = regStack.back();
        regStack.pop_back();
        asmjit::x86::Gp a = regStack.back();
        regStack.pop_back();
        asmjit::x86::Gp res = cc.new_gp64("mulRes");
        cc.mov(res, a);
        cc.imul(res, b);
        regStack.push_back(res);
        break;
      }
      case OpCode::DIV_INT: {
        if (regStack.size() < 2) return std::nullopt;
        asmjit::x86::Gp b = regStack.back();
        regStack.pop_back();
        asmjit::x86::Gp a = regStack.back();
        regStack.pop_back();
        // Safe division: handled in registers with fallback if b == 0
        asmjit::x86::Gp quot = cc.new_gp64("quot");
        asmjit::x86::Gp rem = cc.new_gp64("rem");
        cc.xor_(quot, quot);
        asmjit::Label skipDiv = cc.new_label();
        cc.test(b, b);
        cc.jz(skipDiv);
        cc.mov(quot, a);
        cc.xor_(rem, rem);
        cc.idiv(rem, quot, b);
        cc.bind(skipDiv);
        regStack.push_back(quot);
        break;
      }
      case OpCode::MOD_INT: {
        if (regStack.size() < 2) return std::nullopt;
        asmjit::x86::Gp b = regStack.back();
        regStack.pop_back();
        asmjit::x86::Gp a = regStack.back();
        regStack.pop_back();
        asmjit::x86::Gp quot = cc.new_gp64("quot");
        asmjit::x86::Gp rem = cc.new_gp64("rem");
        cc.xor_(rem, rem);
        asmjit::Label skipMod = cc.new_label();
        cc.test(b, b);
        cc.jz(skipMod);
        cc.mov(quot, a);
        cc.xor_(rem, rem);
        cc.idiv(rem, quot, b);
        cc.bind(skipMod);
        regStack.push_back(rem);
        break;
      }
      case OpCode::CMP_GT_INT: {
        if (regStack.size() < 2) return std::nullopt;
        asmjit::x86::Gp b = regStack.back();
        regStack.pop_back();
        asmjit::x86::Gp a = regStack.back();
        regStack.pop_back();
        asmjit::x86::Gp cond = cc.new_gp64("gtCond");
        cc.cmp(a, b);
        cc.setg(cond.r8());
        cc.movzx(cond, cond.r8());
        regStack.push_back(cond);
        break;
      }
      case OpCode::CMP_GE_INT: {
        if (regStack.size() < 2) return std::nullopt;
        asmjit::x86::Gp b = regStack.back();
        regStack.pop_back();
        asmjit::x86::Gp a = regStack.back();
        regStack.pop_back();
        asmjit::x86::Gp cond = cc.new_gp64("geCond");
        cc.cmp(a, b);
        cc.setge(cond.r8());
        cc.movzx(cond, cond.r8());
        regStack.push_back(cond);
        break;
      }
      case OpCode::CMP_LT_INT: {
        if (regStack.size() < 2) return std::nullopt;
        asmjit::x86::Gp b = regStack.back();
        regStack.pop_back();
        asmjit::x86::Gp a = regStack.back();
        regStack.pop_back();
        asmjit::x86::Gp cond = cc.new_gp64("ltCond");
        cc.cmp(a, b);
        cc.setl(cond.r8());
        cc.movzx(cond, cond.r8());
        regStack.push_back(cond);
        break;
      }
      case OpCode::CMP_LE_INT: {
        if (regStack.size() < 2) return std::nullopt;
        asmjit::x86::Gp b = regStack.back();
        regStack.pop_back();
        asmjit::x86::Gp a = regStack.back();
        regStack.pop_back();
        asmjit::x86::Gp cond = cc.new_gp64("leCond");
        cc.cmp(a, b);
        cc.setle(cond.r8());
        cc.movzx(cond, cond.r8());
        regStack.push_back(cond);
        break;
      }
      case OpCode::CMP_EQ_INT: {
        if (regStack.size() < 2) return std::nullopt;
        asmjit::x86::Gp b = regStack.back();
        regStack.pop_back();
        asmjit::x86::Gp a = regStack.back();
        regStack.pop_back();
        asmjit::x86::Gp cond = cc.new_gp64("eqCond");
        cc.cmp(a, b);
        cc.sete(cond.r8());
        cc.movzx(cond, cond.r8());
        regStack.push_back(cond);
        break;
      }
      case OpCode::CMP_NE_INT: {
        if (regStack.size() < 2) return std::nullopt;
        asmjit::x86::Gp b = regStack.back();
        regStack.pop_back();
        asmjit::x86::Gp a = regStack.back();
        regStack.pop_back();
        asmjit::x86::Gp cond = cc.new_gp64("neCond");
        cc.cmp(a, b);
        cc.setne(cond.r8());
        cc.movzx(cond, cond.r8());
        regStack.push_back(cond);
        break;
      }
      case OpCode::LOAD_COL_ID: {
        // Like `LOAD_COL_INT`, but push the raw 64-bit `ValueId` bits
        // without integer unpacking.
        size_t colIdx = 0;
        for (size_t i = 0; i < referencedCols.size(); ++i) {
          if (referencedCols[i] == static_cast<ColumnIndex>(inst.arg)) {
            colIdx = i;
            break;
          }
        }
        asmjit::x86::Gp rawVal = cc.new_gp64("rawId");
        cc.mov(rawVal, asmjit::x86::qword_ptr(colBaseRegs[colIdx], idx, 3));
        regStack.push_back(rawVal);
        break;
      }
      case OpCode::CMP_EQ_ID: {
        if (regStack.size() < 2) return std::nullopt;
        asmjit::x86::Gp b = regStack.back();
        regStack.pop_back();
        asmjit::x86::Gp a = regStack.back();
        regStack.pop_back();
        asmjit::x86::Gp cond = cc.new_gp64("eqIdCond");
        cc.cmp(a, b);
        cc.sete(cond.r8());
        cc.movzx(cond, cond.r8());
        regStack.push_back(cond);
        break;
      }
      case OpCode::IN_ID_RANGE: {
        if (regStack.empty()) return std::nullopt;
        if (static_cast<size_t>(inst.arg) >= program.idRanges().size()) {
          return std::nullopt;
        }
        const auto& [lo, hi] =
            program.idRanges().at(static_cast<size_t>(inst.arg));
        asmjit::x86::Gp a = regStack.back();
        regStack.pop_back();
        asmjit::x86::Gp loReg = cc.new_gp64("rangeLo");
        asmjit::x86::Gp hiReg = cc.new_gp64("rangeHi");
        asmjit::x86::Gp cond = cc.new_gp64("rangeCond");
        // The bounds are `VocabIndex` IDs, so the sign bit is never set and
        // the bit patterns survive the cast unchanged.
        cc.mov(loReg, static_cast<int64_t>(lo));
        cc.mov(hiReg, static_cast<int64_t>(hi));
        // Unsigned comparison: `lo <= a < hi` in `ValueId` bit order.
        asmjit::Label rangeFail = cc.new_label();
        cc.xor_(cond, cond);
        cc.cmp(a, loReg);
        cc.jb(rangeFail);
        cc.cmp(a, hiReg);
        cc.jae(rangeFail);
        cc.mov(cond, 1);
        cc.bind(rangeFail);
        regStack.push_back(cond);
        break;
      }
      case OpCode::OR_BOOL: {
        if (regStack.size() < 2) return std::nullopt;
        asmjit::x86::Gp b = regStack.back();
        regStack.pop_back();
        asmjit::x86::Gp a = regStack.back();
        regStack.pop_back();
        cc.or_(a, b);
        regStack.push_back(a);
        break;
      }
      case OpCode::RET:
        break;
    }
  }

  // Top of stack is the boolean filter outcome for the current row
  if (!regStack.empty()) {
    asmjit::x86::Gp resultCond = regStack.back();
    asmjit::x86::Gp one = cc.new_gp64("one");
    cc.mov(one, 1);
    cc.shl(one, idx.r8());

    // If resultCond != 0, set bit in matchMask and increment totalMatches
    asmjit::Label skipBit = cc.new_label();
    cc.test(resultCond, resultCond);
    cc.jz(skipBit);
    cc.or_(matchMask, one);
    cc.inc(totalMatches);
    cc.bind(skipBit);
  }

  cc.inc(idx);
  cc.jmp(loopStart);

  cc.bind(loopEnd);
  // Store computed 64-bit match mask
  cc.mov(asmjit::x86::qword_ptr(outMaskPtr), matchMask);
  cc.ret(totalMatches);
  cc.end_func();

  cc.finalize();

  NativeFilterMorselFn nativeFn = nullptr;
  asmjit::Error err = rt->add(&nativeFn, &code);
  if (err != asmjit::Error::kOk || !nativeFn) {
    return std::nullopt;
  }

  return JitCompiledExpression(rt, nativeFn, referencedCols);
}

}  // namespace ql::engine::jit
