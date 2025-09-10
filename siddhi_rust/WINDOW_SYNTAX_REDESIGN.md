# Window Syntax Redesign Proposal

## Problem Statement

The current window syntax `#window.length(10)` creates parsing conflicts that prevent float literal support. The `.` operator is treated as an explicit token by LALRPOP, taking precedence over regex patterns for floating-point numbers like `100.0`.

This is a **critical blocker** for real-world applications requiring decimal precision.

## Current Problematic Syntax

```siddhi
-- These patterns cause float parsing conflicts
from StockStream#window.length(10)
from StockStream#window.time(5 min)
from StockStream#window.lengthBatch(10)
from StockStream#window.timeBatch(30 sec)
from StockStream#window.session(2 min)
from StockStream#window.sort(10, price desc)

-- This fails to parse due to conflicts
from StockStream[price > 100.0]  -- ERROR: Cannot parse 100.0
```

## Proposed Solutions

### Option A: Colon Separator (Recommended) ⭐
**Syntax**: `#window:type(params)`

```siddhi
from StockStream#window:length(10)
from StockStream#window:time(5 min)  
from StockStream#window:lengthBatch(10)
from StockStream#window:timeBatch(30 sec)
from StockStream#window:session(2 min)
from StockStream#window:sort(10, price desc)

-- Now this works!
from StockStream[price > 100.0] -- ✅ Parses correctly
```

**Pros**:
- Eliminates `.` token conflicts
- Maintains familiar `#window` prefix
- Clear namespace separation with `:`
- Minimal migration effort
- Consistent with other namespace patterns

**Cons**:
- Slightly different from Java Siddhi
- Requires backward compatibility layer

### Option B: Function Style
**Syntax**: `#type_window(params)`

```siddhi
from StockStream#length_window(10)
from StockStream#time_window(5 min)
from StockStream#lengthBatch_window(10)  
from StockStream#timeBatch_window(30 sec)
from StockStream#session_window(2 min)
from StockStream#sort_window(10, price desc)
```

**Pros**:
- No token conflicts
- Self-documenting window types
- Shorter syntax

**Cons**:
- Breaks from Java Siddhi convention
- Longer window type names
- More invasive migration

### Option C: Parameterized Style
**Syntax**: `#window(type="name", ...params)`

```siddhi
from StockStream#window(type="length", size=10)
from StockStream#window(type="time", duration="5 min")
from StockStream#window(type="lengthBatch", size=10)
from StockStream#window(type="session", timeout="2 min")
```

**Pros**:
- Most flexible
- Extensible parameter system
- No token conflicts

**Cons**:
- Most verbose
- Significant departure from Java Siddhi
- Complex parameter parsing

## Detailed Design: Option A (Colon Separator)

### Grammar Changes Required

```lalrpop
// Current problematic syntax
// "window" "." <type:Ident> "(" <params> ")"

// New syntax
"window" ":" <type:Ident> "(" <params> ")"
```

### Migration Strategy

#### Phase 1: Dual Support (3 months)
- Support both syntaxes simultaneously
- New syntax is preferred in documentation
- Old syntax shows deprecation warnings

```rust
// Parser supports both
from StockStream#window.length(10)    // ⚠️ DEPRECATED
from StockStream#window:length(10)    // ✅ PREFERRED
```

#### Phase 2: Migration Period (6 months)  
- Old syntax triggers warnings but still works
- Migration tool converts queries automatically
- Community support for migration

#### Phase 3: Deprecation (3 months)
- Old syntax generates parse errors
- Final deadline for migration
- Documentation updated completely

### Implementation Plan

#### Week 1: Core Grammar Update
- [ ] Update LALRPOP grammar rules
- [ ] Add colon-based window parsing
- [ ] Maintain backward compatibility

#### Week 2: Parser Integration
- [ ] Update expression parsing for floats
- [ ] Add comprehensive numeric literal tests
- [ ] Ensure no regressions

#### Week 3: Migration Tooling
- [ ] Create automated query converter
- [ ] Add validation for converted queries
- [ ] Build CLI migration tool

#### Week 4: Testing & Documentation
- [ ] Comprehensive test suite
- [ ] Update all documentation
- [ ] Create migration guide

### Example Conversions

```siddhi
-- BEFORE (problematic)
@app:name('StockAnalysis')
define stream StockStream (symbol string, price float);

from StockStream#window.length(10)[price > 100.0]  -- FAILS TO PARSE
select symbol, avg(price) as avgPrice
insert into AvgPriceStream;

-- AFTER (fixed)
@app:name('StockAnalysis')
define stream StockStream (symbol string, price float);

from StockStream#window:length(10)[price > 100.0]  -- ✅ WORKS
select symbol, avg(price) as avgPrice
insert into AvgPriceStream;
```

### Backward Compatibility Implementation

```rust
// Support both patterns during transition
WindowSyntax = {
    "#window:" <type:Ident> "(" <params> ")" => WindowExpr::new(type, params),
    "#window." <type:Ident> "(" <params> ")" => {
        emit_deprecation_warning("window.syntax", "Use #window: instead of #window.");
        WindowExpr::new(type, params)
    }
}
```

### Performance Impact

- **Parsing Performance**: Minimal impact, single token change
- **Runtime Performance**: No impact, same AST structure
- **Memory Usage**: No additional memory required

### Testing Strategy

#### Unit Tests
- [ ] All window types with new syntax
- [ ] Float literal parsing in expressions
- [ ] Backward compatibility validation
- [ ] Error message quality

#### Integration Tests
- [ ] Complex queries with multiple windows
- [ ] Nested expressions with floats
- [ ] Real-world Siddhi applications
- [ ] Migration tool validation

#### Performance Tests
- [ ] Parse time benchmarks
- [ ] Memory usage validation
- [ ] Large query parsing

## Risk Analysis

### High Risk
- **Breaking Changes**: Old queries stop working after deprecation
- **Migration Complexity**: Large codebases may struggle

### Medium Risk  
- **Tool Compatibility**: External tools may need updates
- **Documentation Gap**: Period where docs are inconsistent

### Low Risk
- **Performance Regression**: Unlikely with minimal grammar changes
- **Security Issues**: No security implications

### Mitigation Strategies

1. **Extensive Testing**: Comprehensive test suite covering edge cases
2. **Gradual Rollout**: Long transition period with clear communication
3. **Migration Support**: Automated tools and community help
4. **Rollback Plan**: Ability to revert if major issues arise

## Alternative Approaches Considered

### Switch Parser Generator
- **Nom**: More control, better error handling, but complete rewrite
- **Pest**: Cleaner grammar, but still significant work
- **Hand-written**: Total control, but massive implementation effort

### Custom Lexer
- **Context-aware tokenization**: Solve conflicts at lexer level
- **Preprocessing**: Handle floats before parsing
- **Two-phase parsing**: Separate tokenization and parsing

These were rejected due to implementation complexity and maintenance burden.

## Success Metrics

1. **Parser Success Rate**: >99.9% on real-world queries
2. **Migration Success**: >95% automated conversion accuracy
3. **Performance**: <5% parsing overhead during transition
4. **User Satisfaction**: Positive community feedback on new syntax
5. **Adoption Rate**: >80% of users migrate within 6 months

## Timeline Summary

| Phase | Duration | Key Deliverable |
|-------|----------|-----------------|
| Design | 1 week | Finalized syntax specification |
| Implementation | 4 weeks | Working parser with dual support |
| Testing | 2 weeks | Comprehensive validation |
| Migration Tools | 2 weeks | Automated conversion utilities |
| Documentation | 1 week | Complete migration guide |
| **Total** | **10 weeks** | **Production-ready redesign** |

## Community Impact

### Positive
- **Unblocks float literals**: Major functionality restored
- **Cleaner syntax**: More consistent window operations
- **Future-proof**: Better foundation for parser evolution

### Negative
- **Migration burden**: Users must update existing queries
- **Learning curve**: New syntax to memorize
- **Tool updates**: IDE/editor support needs updates

## Recommendation

**Proceed with Option A (Colon Separator)** as it provides the best balance of:
- ✅ **Technical feasibility** (straightforward implementation)
- ✅ **Migration simplicity** (minimal syntax changes)
- ✅ **User experience** (familiar patterns)
- ✅ **Future compatibility** (extensible design)

The 10-week implementation timeline is reasonable and the 12-month migration period provides adequate time for ecosystem adaptation.

---

*Document Version: 1.0*  
*Created: 2025-01-06*  
*Status: Design Proposal*  
*Owner: Siddhi Rust Team*