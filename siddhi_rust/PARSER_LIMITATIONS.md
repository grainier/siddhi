# Siddhi Rust Parser Limitations & Known Issues

This document catalogs all known limitations and issues with the Siddhi Rust parser implementation, along with proposed solutions and workarounds.

## 🔴 Critical Limitations

### 1. Float Literal Token Conflict
**Issue**: Floating-point literals (e.g., `100.0`, `3.14`) fail to parse due to LALRPOP token precedence rules.

**Root Cause**: 
- LALRPOP gives explicit tokens (like `.` in `#window.length`) precedence over regex patterns
- The `.` in float literals conflicts with the `.` used in window syntax (`#window.length`)
- This is an architectural limitation of LALRPOP parser generator

**Impact**: 
- Cannot use decimal numbers in queries
- Blocks financial/scientific applications requiring precision

**Current Workaround**: 
- Use integers only (e.g., `100` instead of `100.0`)
- Scale values if decimal precision needed

**Proposed Solutions**:
1. **Redesign window syntax** to avoid `.` operator (recommended)
2. **Switch parser generator** from LALRPOP to Nom or Pest
3. **Custom lexer** to handle context-sensitive tokenization

### 2. Window Syntax Design Conflict
**Issue**: The `#window.length()` syntax creates parsing ambiguities.

**Problems**:
- Conflicts with float literals
- Inconsistent with other Siddhi syntax patterns
- Makes grammar more complex

**Proposed Redesign**:
```siddhi
# Current problematic syntax
from StockStream#window.length(10)
from StockStream#window.time(5 min)

# Proposed new syntax (Option A - Function style)
from StockStream#length_window(10)
from StockStream#time_window(5 min)

# Proposed new syntax (Option B - Parameterized style)
from StockStream#window(type="length", size=10)
from StockStream#window(type="time", duration="5 min")

# Proposed new syntax (Option C - Colon separator)
from StockStream#window:length(10)
from StockStream#window:time(5 min)
```

## 🟠 Moderate Limitations

### 3. Limited Aggregate Function Support
**Issue**: Missing support for many aggregate functions in expression parsing.

**Missing Functions**:
- `stddev()`, `variance()`
- `percentile()`, `median()`
- `first()`, `last()`
- `collect()`, `group_concat()`

**Impact**: Complex analytical queries cannot be parsed

### 4. Output Rate Limiting Syntax
**Issue**: `output every X events/seconds` clause not fully supported.

**Missing Patterns**:
- `output first every 10 events`
- `output last every 5 sec`
- `output all every 2 min`
- `output snapshot every 1 hour`

### 5. Pattern Query Limitations
**Issue**: Complex pattern matching syntax incomplete.

**Missing Features**:
- Logical patterns with `and`, `or`, `not`
- Counting patterns `<3:5>`
- Within clause for patterns
- Absent event patterns

## 🟡 Minor Limitations

### 6. Script Executor Support
**Issue**: No support for embedded scripts.

**Missing**:
- JavaScript expressions: `js:customFunction()`
- R scripts: `r:statisticalModel()`
- Python scripts: `python:mlPredict()`

### 7. Advanced Window Types
**Issue**: Many window types not recognized in grammar.

**Missing Windows**:
- `#window.unique(symbol)`
- `#window.batch(10)`
- `#window.lengthBatch(5, 3)`
- `#window.cron("0 0 * * *")`
- `#window.delay(2 sec)`
- `#window.externalTime(timestamp)`

### 8. Table Index Syntax
**Issue**: Index hints and forced index usage not supported.

**Missing**:
- `from StockTable[symbol == 'WSO2' using index symbol_idx]`
- `from OrderTable force index (customer_idx)`

## 🟢 Enhancement Opportunities

### 9. Error Recovery
**Current**: Parser fails on first error.

**Desired**: 
- Continue parsing after errors
- Collect multiple errors
- Provide fix suggestions

### 10. Syntax Extensions
**Missing Modern Features**:
- Lambda expressions
- List comprehensions
- Named parameters
- Optional chaining

## 📊 Impact Assessment

| Limitation | Severity | Frequency | User Impact | Priority |
|------------|----------|-----------|-------------|----------|
| Float Literals | HIGH | Very Common | Blocks real apps | P0 |
| Window Syntax | HIGH | Common | Confusing errors | P0 |
| Aggregate Functions | MEDIUM | Common | Feature gaps | P1 |
| Output Rate | MEDIUM | Occasional | Missing features | P2 |
| Pattern Queries | MEDIUM | Rare | Advanced use cases | P2 |
| Script Support | LOW | Rare | Alternative exists | P3 |
| Advanced Windows | LOW | Occasional | Workarounds exist | P3 |
| Table Indexes | LOW | Rare | Performance only | P3 |

## 🔧 Recommended Action Plan

### Phase 1: Critical Fixes (Q1 2025)
1. **Redesign window syntax** to eliminate `.` operator
2. **Implement float literal support** with new window syntax
3. **Add comprehensive tests** for numeric literals

### Phase 2: Feature Completion (Q2 2025)
1. **Add missing aggregate functions**
2. **Implement output rate limiting**
3. **Complete pattern query support**

### Phase 3: Advanced Features (Q3 2025)
1. **Script executor integration**
2. **Advanced window types**
3. **Table optimization hints**

### Phase 4: Parser Evolution (Q4 2025)
1. **Error recovery mechanisms**
2. **Modern syntax extensions**
3. **Performance optimizations**

## 🚀 Migration Strategy

### For Window Syntax Redesign

1. **Backward Compatibility Phase** (3 months)
   - Support both old and new syntax
   - Emit deprecation warnings for old syntax
   - Provide automated migration tool

2. **Migration Phase** (6 months)
   - Old syntax triggers warnings
   - Documentation uses new syntax
   - Community migration support

3. **Deprecation Phase** (3 months)
   - Old syntax generates errors
   - Final migration deadline

## 💡 Alternative Solutions

### Option 1: Switch Parser Technology
**Pros**:
- Nom: More control, better error handling
- Pest: Cleaner grammar, better debugging
- Hand-written: Complete control

**Cons**:
- Significant rewrite effort
- Loss of LALRPOP benefits
- Testing overhead

### Option 2: Two-Phase Parsing
**Approach**:
1. Lexer phase handles numbers specially
2. Parser phase uses preprocessed tokens

**Pros**:
- Solves float literal issue
- Keeps LALRPOP

**Cons**:
- Added complexity
- Maintenance overhead

### Option 3: Context-Aware Lexing
**Approach**:
- Lexer tracks context (in window vs expression)
- Returns different tokens based on context

**Pros**:
- Solves ambiguity
- No syntax changes

**Cons**:
- Complex implementation
- Harder to maintain

## 📈 Success Metrics

- **Parser Success Rate**: >99.9% on real queries
- **Error Message Quality**: Users fix 90% of errors without help
- **Performance**: <1ms parse time for typical queries
- **Feature Coverage**: 100% of Java Siddhi syntax supported

## 🔍 Testing Requirements

### Test Coverage Needed
- Numeric literal edge cases
- Window syntax variations
- Complex nested expressions
- Error recovery scenarios
- Performance benchmarks

### Regression Prevention
- Automated test suite for all fixes
- Grammar validation tests
- Backward compatibility tests
- Performance regression tests

---

*Last Updated: 2025-01-06*
*Status: Active Development*
*Owner: Siddhi Rust Team*