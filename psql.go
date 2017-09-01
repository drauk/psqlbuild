// src/go/psql.go   2017-9-1   Alan U. Kennington.
// $Id: psql.go 46544 2017-08-30 14:37:02Z akenning $
// PostgreSQL query builder for first test program for learning "go".
// Using version go1.1.2.
// PostgreSQL version 8.0.3.
/*-------------------------------------------------------------------------
Functions in this package.

psql_expr_node{}
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
psql_expr_parent::
psql_expr_parent::getParent
psql_expr_parent::setParent
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
psql_expr_str::
psql_expr_str::clone
psql_expr_str::Set
psql_expr_str::Build
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
psql_expr_num::
psql_expr_num::clone
psql_expr_num::Set
psql_expr_num::Build
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
psql_expr_field::
psql_expr_field::clone
psql_expr_field::Set
psql_expr_field::String
psql_expr_field::Build
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
psql_expr_op::
psql_expr_op::clone
psql_expr_op::Clear
psql_expr_op::Set
psql_expr_op::Build
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
psql_expr_fn::
psql_expr_fn::clone
psql_expr_fn::Clear
psql_expr_fn::ArgCount
psql_expr_fn::ArgCountValid
psql_expr_fn::Set
psql_expr_fn::Build
---------------------------------------------------------------------------
Psql_expr::
Psql_expr::setNode
Psql_expr::getNode
Psql_expr::SetStr
Psql_expr::SetNum
Psql_expr::SetFld
Psql_expr::SetOp
Psql_expr::SetFn
Psql_expr::Clone
Psql_expr::Build
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Xstr
Xnum
Xfld
Xop
Xfn
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
psql_expr_as::
psql_expr_as::Set
psql_expr_as::Build
---------------------------------------------------------------------------
psql_from_node{}
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
psql_from_table::
psql_from_table::getParent
psql_from_table::setParent
psql_from_table::Set
psql_from_table::SetOnly
psql_from_table::Build
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
psql_from_item::
psql_from_item::Set
psql_from_item::Build
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Psql_from::
Psql_from::setNode
Psql_from::SetTable
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Xtable
---------------------------------------------------------------------------
psql_order::
psql_order::SetDirn
psql_order::Set
psql_order::SetDesc
psql_order::Build
---------------------------------------------------------------------------
psql_limit::
psql_limit::Set
psql_limit::Build
---------------------------------------------------------------------------
Psql_select::
Psql_select::Clear
Psql_select::SelectAppendStrAs
Psql_select::SelectAppendStr
Psql_select::SelectAppendNumAs
Psql_select::SelectAppendNum
Psql_select::SelectAppendFldAs
Psql_select::SelectAppendFld
Psql_select::SelectAppendOpAs
Psql_select::SelectAppendOp
Psql_select::SelectAppendFnAs
Psql_select::SelectAppendFn
Psql_select::SelectAppendExprAs
Psql_select::SelectAppendExpr
Psql_select::SelectAppendExprs
Psql_select::SelectBuild
Psql_select::FromAppendItem
Psql_select::FromBuild
Psql_select::WhereSetExpr
Psql_select::OrderAppendDirn
Psql_select::OrderAppend
Psql_select::OrderAppendDesc
Psql_select::OrderBuild
Psql_select::LimitSet
Psql_select::Build
-------------------------------------------------------------------------*/

/*
A Go-package to build PostgreSQL query strings safely and conveniently.

The convenience is most noticeable for very long complex queries. The safety is
essential for preventing SQL injections. In principle, no SQL should ever be
written by hand. The SQL language is too dangerous!
*/
package psqlbuild

// External libraries.
import "fmt"

// import "net/http"
// import "log"
import "io"

// import "time"
import "strings"
import "strconv"

// import "errors"

// import . "localhost/s2list"
import . "github.com/drauk/s2list"

// import "localhost/elist"
import "github.com/drauk/elist"

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
const formatFloat string = "%.17g"

// Bad kludge. Useful for tracing functions. Assumes HTML formatting.
var W_kludge_psql io.Writer

// Activate W_kludge_psql.
var W_kludge_psql_on bool = false

//=============================================================================
//=============================================================================

type psql_expr_node interface {
    //----------------------//
    //   psql_expr_node{}   //
    //----------------------//
    /*------------------------------------------------------------------------------
      Common interface for all expression tree-nodes.
      Union of *psql_expr_str/num/field/op/fn.
      ------------------------------------------------------------------------------*/
    getParent() psql_expr_node
    setParent(psql_expr_node)
    Build() (string, error)
}   // End of struct psql_expr_node{}.

//=============================================================================
//=============================================================================

type psql_expr_parent struct {
    //----------------------//
    //  psql_expr_parent::  //
    //----------------------//
    /*------------------------------------------------------------------------------
      Parent class to be embedded in all psql_expr_node classes.
      This is effectively a parent-class for all psql_expr_node classes.
      So the code for setParent() and getParent() doesn't need multiple copies.
      ------------------------------------------------------------------------------*/
    parent psql_expr_node
}

func (p *psql_expr_parent) getParent() psql_expr_node {
    //------------------------------//
    //  psql_expr_parent::getParent //
    //------------------------------//
    if p == nil {
        return nil
    }
    return p.parent
}   // End of function psql_expr_parent::getParent.

func (p *psql_expr_parent) setParent(q psql_expr_node) {
    //------------------------------//
    //  psql_expr_parent::setParent //
    //------------------------------//
    if p == nil {
        return
    }
    p.parent = q
}   // End of function psql_expr_parent::setParent.

//=============================================================================
//=============================================================================

/*------------------------------------------------------------------------------
Literal string, which must be quoted in the PostgreSQL style.
------------------------------------------------------------------------------*/
type psql_expr_str struct {
    //----------------------//
    //    psql_expr_str::   //
    //----------------------//
    val string // Literal string. Will be quoted carefully.
    psql_expr_parent
}

/*------------------------------------------------------------------------------
Clone a string expression.
------------------------------------------------------------------------------*/
func (p *psql_expr_str) clone() *psql_expr_str {
    //----------------------//
    // psql_expr_str::clone //
    //----------------------//
    if p == nil {
        return nil
    }
    q := new(psql_expr_str)
    q.val = p.val
    return q
}   // End of function psql_expr_str::clone.

/*------------------------------------------------------------------------------
Set a constant string expression.
------------------------------------------------------------------------------*/
func (p *psql_expr_str) Set(s string) error {
    //----------------------//
    //  psql_expr_str::Set  //
    //----------------------//
    if p == nil {
        return elist.New("psql_expr_str::Set: p == nil")
    }
    p.val = s
    return nil
}   // End of function psql_expr_str::Set.

/*------------------------------------------------------------------------------
For the Postgres standard for string escape characters, see:
https://www.postgresql.org/docs/8.0/static/sql-syntax.html#SQL-SYNTAX-CONSTANTS
------------------------------------------------------------------------------*/
func (p *psql_expr_str) Build() (string, error) {
    //--------------------------//
    //   psql_expr_str::Build   //
    //--------------------------//
    if p == nil {
        return "", elist.New("psql_expr_str::Build: p == nil")
    }
    var str string

    // The extremely important rule for PostgreSQL strings.
    // This is where the SQL injections are (hopefully) defeated!
    // old: var rep = strings.NewReplacer("'", "\'", `"`, `\"`, `\`, `\\`);
    var rep = strings.NewReplacer("'", "''", `\`, `\\`)
    str = fmt.Sprintf("'%s'", rep.Replace(p.val))

    return str, nil
}   // End of function psql_expr_str::Build.

//=============================================================================
//=============================================================================

/*------------------------------------------------------------------------------
A numerical expression value is inserted into SQL queries as a string.
The validity of that string _must_ be verified before setting the
member "val" of psql_expr_num.
------------------------------------------------------------------------------*/
type psql_expr_num struct {
    //----------------------//
    //    psql_expr_num::   //
    //----------------------//
    val string // String representing a number.
    psql_expr_parent
}

/*------------------------------------------------------------------------------
Clone a number expression.
------------------------------------------------------------------------------*/
func (p *psql_expr_num) clone() *psql_expr_num {
    //----------------------//
    // psql_expr_num::clone //
    //----------------------//
    if p == nil {
        return nil
    }
    q := new(psql_expr_num)
    q.val = p.val
    return q
}   // End of function psql_expr_num::clone.

/*------------------------------------------------------------------------------
Set a constant numerical expression.
The argument "v" may be an integer, float64 or a string which represents
an integer or floating-point constant.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
For PostgreSQL numeric constants, see
https://www.postgresql.org/docs/8.0/static/sql-syntax.html#SQL-SYNTAX-CONSTANTS
See section 4.1.2.4 "Numeric constants".
digits
digits.[digits][e[+-]digits]
[digits].digits[e[+-]digits]
digits e[+-]digits
------------------------------------------------------------------------------*/
func (p *psql_expr_num) Set(v interface{}) error {
    //----------------------//
    //  psql_expr_num::Set  //
    //----------------------//
    if p == nil {
        return elist.New("psql_expr_num::Set: p == nil")
    }
    var str string

    switch x := v.(type) {
    // There must be a better way to do these 12 cases!!!
    case int:
        str = fmt.Sprintf("%d", x)
    case int8:
        str = fmt.Sprintf("%d", x)
    case int16:
        str = fmt.Sprintf("%d", x)
    case int32:
        str = fmt.Sprintf("%d", x)
    case int64:
        str = fmt.Sprintf("%d", x)
    case uint:
        str = fmt.Sprintf("%d", x)
    case uint8:
        str = fmt.Sprintf("%d", x)
    case uint16:
        str = fmt.Sprintf("%d", x)
    case uint32:
        str = fmt.Sprintf("%d", x)
    case uint64:
        str = fmt.Sprintf("%d", x)
    case float32:
        // NOTE: Should check that the Go-format is acceptable to PostgreSQL.
        str = fmt.Sprintf(formatFloat, x)
    case float64:
        // NOTE: Should check that the Go-format is acceptable to PostgreSQL.
        str = fmt.Sprintf(formatFloat, x)
    case string:
        // Test whether this is a genuine numerical string.
        // Testing for integer format is probably fairly safe.
        var E error
        _, E = strconv.ParseInt(x, 10, 0)
        if E == nil {
            str = x
            break
        }
        // Testing for float format is probably not so safe.
        // A valid Go-format might not be a valid PostgreSQL-format.
        _, E = strconv.ParseFloat(x, 64)
        if E == nil {
            str = x
            break
        }
        return elist.New(
            "psql_expr_num::Set: string does not represent a number")
    default:
        return elist.New("psql_expr_num::Set: string equals nil")
    }
    p.val = str
    return nil
}   // End of function psql_expr_num::Set.

/*------------------------------------------------------------------------------
This function trusts that the string parameter is a valid PostgreSQL number.
Otherwise, SQL injections may occur!
------------------------------------------------------------------------------*/
func (p *psql_expr_num) Build() (string, error) {
    //--------------------------//
    //   psql_expr_num::Build   //
    //--------------------------//
    if p == nil {
        return "", elist.New("psql_expr_num::Build: p == nil")
    }
    return p.val, nil
}   // End of function psql_expr_num::Build.

//=============================================================================
//=============================================================================

/*------------------------------------------------------------------------------
Table name syntax is described here.
https://www.postgresql.org/docs/8.0/static/sql-syntax.html
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
PostgreSQL value-expressions are described here:
https://www.postgresql.org/docs/8.0/static/sql-expressions.html
------------------------------------------------------------------------------*/
type psql_expr_field struct {
    //----------------------//
    //   psql_expr_field::  //
    //----------------------//
    tab string // Table name. Empty to indicate not set.
    fld string // The field (i.e. column name) of a table.
    psql_expr_parent
}

/*------------------------------------------------------------------------------
Clone a field expression.
------------------------------------------------------------------------------*/
func (p *psql_expr_field) clone() *psql_expr_field {
    //--------------------------//
    //  psql_expr_field::clone  //
    //--------------------------//
    if p == nil {
        return nil
    }
    q := new(psql_expr_field)
    q.tab = p.tab
    q.fld = p.fld
    return q
}   // End of function psql_expr_field::clone.

func (p *psql_expr_field) Set(tab string, fld string) error {
    //----------------------//
    // psql_expr_field::Set //
    //----------------------//
    if p == nil {
        return elist.New("psql_expr_field::Set: p == nil")
    }
    p.tab = tab
    p.fld = fld
    return nil
}   // End of function psql_expr_field::Set.

/*------------------------------------------------------------------------------
psql_expr_field::String only returns a string representation of this object
for diagnostic purposes.
Not for use in SQL query building!
------------------------------------------------------------------------------*/
func (p *psql_expr_field) String() string {
    //--------------------------//
    //  psql_expr_field::String //
    //--------------------------//
    if p == nil {
        return ""
    }
    return fmt.Sprintf("(table: \"%s\", field: \"%s\")", p.tab, p.fld)
}   // End of function psql_expr_field::String.

/*------------------------------------------------------------------------------
Build a single field for an SQL query.
https://www.postgresql.org/docs/8.0/static/sql-select.html#SQL-SELECT-LIST
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
This routine replaces '"' with '""' to fit the lexical rules for identifiers.
https://www.postgresql.org/docs/8.0/static/sql-syntax.html

"Quoted identifiers can contain any character other than a double quote itself.
(To include a double quote, write two double quotes.) This allows constructing
table or column names that would otherwise not be possible, such as ones
containing spaces or ampersands."
------------------------------------------------------------------------------*/
func (p *psql_expr_field) Build() (string, error) {
    //--------------------------//
    //  psql_expr_field::Build  //
    //--------------------------//
    if p == nil {
        return "", elist.New("psql_expr_num::Set: p == nil")
    }
    // The field (i.e. column) identifier must be non-empty.
    if p.fld == "" {
        return "", elist.New("psql_expr_num::Set: p.fld == nil")
    }
    // The rule for PostgreSQL identifiers is that double-quote is repeated.
    var rep = strings.NewReplacer(`"`, `""`)

    var res, tab string
    if p.tab != "" {
        tab = fmt.Sprintf("\"%s\".", rep.Replace(p.tab))
    }
    res = fmt.Sprintf("%s\"%s\"", tab, rep.Replace(p.fld))
    return res, nil
}   // End of function psql_expr_field::Build.

//=============================================================================
//=============================================================================

type psql_expr_op struct {
    //----------------------//
    //    psql_expr_op::    //
    //----------------------//
    op    string         // The binary or unary operation.
    left  psql_expr_node // The left sub-expression.
    right psql_expr_node // The right sub-expression.
    psql_expr_parent
}

/*------------------------------------------------------------------------------
Clone an operator expression.
------------------------------------------------------------------------------*/
func (p *psql_expr_op) clone() *psql_expr_op {
    //----------------------//
    //  psql_expr_op::clone //
    //----------------------//
    if p == nil {
        return nil
    }
    q := new(psql_expr_op)
    q.op = p.op

    // Clone the left sub-expression recursively.
    switch x := p.left.(type) {
    case *psql_expr_str:
        q.left = x.clone()
    case *psql_expr_num:
        q.left = x.clone()
    case *psql_expr_field:
        q.left = x.clone()
    case *psql_expr_op:
        q.left = x.clone()
    case *psql_expr_fn:
        // NOTE: To be defined!
        q.left = x.clone()
    case nil:
    default:
        // "None of the above" is an error.
        return nil
    }

    // Clone the right sub-expression recursively.
    switch x := p.right.(type) {
    case *psql_expr_str:
        q.right = x.clone()
    case *psql_expr_num:
        q.right = x.clone()
    case *psql_expr_field:
        q.right = x.clone()
    case *psql_expr_op:
        q.right = x.clone()
    case *psql_expr_fn:
        // NOTE: To be defined!
        q.right = x.clone()
    case nil:
    default:
        // "None of the above" is an error.
        return nil
    }

    return q
}   // End of function psql_expr_op::clone.

func (p *psql_expr_op) Clear() error {
    //----------------------//
    //  psql_expr_op::Clear //
    //----------------------//
    if p == nil {
        return elist.New("psql_expr_op::Clear: p == nil")
    }
    p.op = ""
    p.left = nil
    p.right = nil
    return nil
}   // End of function psql_expr_op::Clear.

/*------------------------------------------------------------------------------
This function makes only a very rudimentary check of the correctness of
the specified operator expression.
It has many false positives and false negatives.
So don't open up this function to users, especially hackers!
It should at least accept most correct operator-expressions for my purposes.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Logic operators:
https://www.postgresql.org/docs/8.0/static/functions.html#FUNCTIONS-LOGICAL
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Possible characters in operators:
https://www.postgresql.org/docs/8.0/static/sql-syntax.html#SQL-SYNTAX-OPERATORS
+ - * / < > = ~ ! @ # % ^ & | ` ?
------------------------------------------------------------------------------*/
func (p *psql_expr_op) Set(op string,
    //----------------------//
    //   psql_expr_op::Set  //
    //----------------------//
    lft psql_expr_node, rt psql_expr_node) error {
    if p == nil {
        return elist.New("psql_expr_op::Set: p == nil")
    }
    p.Clear()

    // Check binary operators.
    if lft != nil && rt != nil {
        switch op {
        // Infix operators. Arithmetic.
        case "+", "-", "*", "/", "%", "^":
        // Comparison.
        case "=", "<", ">", "<=", ">=", "<>", "!=":
        // String matching.
        case "~", "~*", "!~", "!~*":
        // String or bit-string concatenation.
        case "||":
        // Bit-string operations.
        case "&", "|", "<<", ">>", "#":
        // Logic. Should be a case-independent test!
        case "and", "or", "AND", "OR":
        default:
            return elist.Newf(
                "psql_expr_op::Set: bad binary operator \"%s\"", op)
        }
        // Check prefix operators.
    } else if lft == nil && rt != nil {
        switch op {
        // Arithmetic.
        case "+", "-":
        // Bit-string operations.
        case "~":
        // Logic. Should be a case-independent test!
        case "not", "NOT":
        default:
            return elist.Newf(
                "psql_expr_op::Set: bad prefix operator \"%s\"", op)
        }
    } else {
        return elist.New("psql_expr_op::Set: left/right nil combination")
    }

    // Set the parent links.
    if lft != nil {
        lft.setParent(p)
    }
    if rt != nil {
        rt.setParent(p)
    }
    p.op = op
    p.left = lft
    p.right = rt
    return nil
}   // End of function psql_expr_op::Set.

/*------------------------------------------------------------------------------
This function must recursively traverse the expression tree.
It is assumed that the operator and the left/right expressions are valid.
Only prefix-unary and infix-binary operators are implemented here.
------------------------------------------------------------------------------*/
func (p *psql_expr_op) Build() (string, error) {
    //----------------------//
    //  psql_expr_op::Build //
    //----------------------//
    if p == nil {
        return "", elist.New("psql_expr_op::Set: p == nil")
    }
    if p.op == "" {
        return "", elist.New("psql_expr_op::Set: p.op == nil")
    }
    var E error
    var lft, rt, result string

    // Build the left expression.
    if p.left != nil {
        lft, E = p.left.Build()
        if E != nil {
            return "", elist.Push(E, "psql_expr_op::Build: p.left.Build()")
        }
    }
    // Build the right expression.
    if p.right != nil {
        rt, E = p.right.Build()
        if E != nil {
            return "", elist.Push(E, "psql_expr_op::Build: p.right.Build()")
        }
    }
    // Construct the resultant query expression.
    if p.left == nil && p.right != nil {
        // Prefix-unary case.
        result = fmt.Sprintf("%s(%s)", p.op, rt)
    } else if p.left != nil && p.right != nil {
        // Infix-binary case.
        result = fmt.Sprintf("(%s) %s (%s)", lft, p.op, rt)
    } else {
        return "", elist.New("psql_expr_op::Set: wrong left/right nil mix")
    }
    return result, nil
}   // End of function psql_expr_op::Build.

//=============================================================================
//=============================================================================

/*------------------------------------------------------------------------------
The function name must be a non-empty string, which apparently
must contain only ASCII letters and underscores.
https://www.postgresql.org/docs/8.0/static/functions.html
But probably user-defined functions can be virtually any kind of string.
https://www.postgresql.org/docs/8.0/static/sql-createfunction.html
Putting double-quotes around function names seems to be the right thing to do!
The name-formation-rules for identifiers and key words probably apply here.
https://
www.postgresql.org/docs/8.0/static/sql-syntax.html#SQL-SYNTAX-IDENTIFIERS
Thus initial character is a letter or underscore, but letters can be non-Latin.
Other letters may also be digits or dollar signs.
------------------------------------------------------------------------------*/
type psql_expr_fn struct {
    //----------------------//
    //    psql_expr_fn::    //
    //----------------------//
    fn   string    // The function name.
    args List_base // The function arguments.
    psql_expr_parent
}

/*------------------------------------------------------------------------------
Clone a function expression.
------------------------------------------------------------------------------*/
func (p *psql_expr_fn) clone() *psql_expr_fn {
    //----------------------//
    //  psql_expr_fn::clone //
    //----------------------//
    if p == nil {
        return nil
    }
    q := new(psql_expr_fn)
    q.fn = p.fn

    // Some functions may have zero arguments.
    if p.args.Empty() {
        return q
    }
    var E error
    var iter List_iter
    var curr *List_node

    E = iter.Init(&p.args)
    if E != nil {
        return nil
    }
    for curr, E = iter.Next(); curr != nil; curr, E = iter.Next() {
        if E != nil {
            return nil
        }
        var v interface{}
        v, E = curr.GetValue()
        if E != nil {
            return nil
        }
        // Construct a clone from each argument.
        switch x := v.(type) {
        case *psql_expr_str:
            arg := x.clone()
            if arg == nil {
                return nil
            }
            E = q.args.AppendValue(arg)
            if E != nil {
                return nil
            }
        case *psql_expr_num:
            arg := x.clone()
            if arg == nil {
                return nil
            }
            E = q.args.AppendValue(arg)
            if E != nil {
                return nil
            }
        case *psql_expr_field:
            arg := x.clone()
            if arg == nil {
                return nil
            }
            E = q.args.AppendValue(arg)
            if E != nil {
                return nil
            }
        case *psql_expr_op:
            // This is recursive!
            arg := x.clone()
            if arg == nil {
                return nil
            }
            E = q.args.AppendValue(arg)
            if E != nil {
                return nil
            }
        case *psql_expr_fn:
            // This is recursive!
            arg := x.clone()
            if arg == nil {
                return nil
            }
            E = q.args.AppendValue(arg)
            if E != nil {
                return nil
            }
        case nil:
            // A correct function must have no nil-arguments.
            return nil
        default:
            // "None of the above" is an error.
            return nil
        }

        // Set the parent node.
        pnode, ok := v.(psql_expr_node)
        if !ok {
            return nil
        }
        pnode.setParent(q)
    }
    return q
}   // End of function psql_expr_fn::clone.

func (p *psql_expr_fn) Clear() error {
    //----------------------//
    //  psql_expr_fn::Clear //
    //----------------------//
    if p == nil {
        return elist.New("psql_expr_fn::Clear: p == nil")
    }
    var E error
    p.fn = ""
    E = p.args.Clear()
    if E != nil {
        return elist.Push(E, "psql_expr_fn::Clear: p == nil")
    }
    return nil
}   // End of function psql_expr_fn::Clear.

/*------------------------------------------------------------------------------
Return the current number of arguments.
------------------------------------------------------------------------------*/
func (p *psql_expr_fn) ArgCount() int {
    //--------------------------//
    //  psql_expr_fn::ArgCount  //
    //--------------------------//
    if p == nil {
        return 0
    }
    return p.args.Length()
}   // End of function psql_expr_fn::ArgCount.

/*------------------------------------------------------------------------------
Return the current nil/wrong/total numbers of arguments.
------------------------------------------------------------------------------*/
func (p *psql_expr_fn) ArgCountValid() (int, int, int) {
    //------------------------------//
    //  psql_expr_fn::ArgCountValid //
    //------------------------------//
    if p == nil {
        return 0, 0, 0
    }
    return p.args.ValidLength()
}   // End of function psql_expr_fn::ArgCountValid.

func (p *psql_expr_fn) Set(name string, args ...psql_expr_node) error {
    //----------------------//
    //   psql_expr_fn::Set  //
    //----------------------//
    if p == nil {
        return elist.New("psql_expr_fn::Set: p == nil")
    }
    var E error

    E = p.Clear()
    if E != nil {
        return elist.New("psql_expr_fn::Set: p.Clear()")
    }
    p.fn = name

    // Some functions may have zero arguments.
    if args == nil {
        return nil
    }

    // Some functions may have one or more arguments.
    for _, q := range args {
        if q == nil {
            return elist.New("psql_expr_fn::Set: range args")
        }
        E = p.args.AppendValue(q)
        if E != nil {
            return elist.Push(E, "psql_expr_fn::Set: p.args.AppendValue()")
        }
        q.setParent(p)
    }
    return nil
}   // End of function psql_expr_fn::Set.

/*------------------------------------------------------------------------------
This function must recursively traverse the expression tree.
It is assumed that the function name and the argument expressions are valid.
------------------------------------------------------------------------------*/
func (p *psql_expr_fn) Build() (string, error) {
    //----------------------//
    //  psql_expr_fn::Build //
    //----------------------//
    if p == nil {
        return "", elist.New("psql_expr_fn::Build: p == nil")
    }
    if p.fn == "" {
        return "", elist.New("psql_expr_fn::Build: p.fn == nil")
    }
    // The rule for PostgreSQL identifiers is that double-quote is repeated.
    var rep = strings.NewReplacer(`"`, `""`)
    var result string
    result = fmt.Sprintf("\"%s\"(", rep.Replace(p.fn))

    var E error
    var iter List_iter
    var curr *List_node

    E = iter.Init(&p.args)
    if E != nil {
        return "", elist.Push(E, "psql_expr_fn::Build: iter.Init()")
    }
    var nloop int = 0
    var arg string

    for curr, E = iter.Next(); curr != nil; curr, E = iter.Next() {
        if E != nil {
            return "", elist.Push(E, "psql_expr_fn::Build: iter.Next()")
        }
        if nloop > 0 {
            // The space is really optional, but it looks better!
            result += ", "
        }
        nloop += 1
        var v interface{}
        v, E = curr.GetValue()
        if E != nil {
            return "", elist.Push(E, "psql_expr_fn::Build: curr.GetValue()")
        }

        // Build the string.
        pnode, ok := v.(psql_expr_node)
        if !ok {
            return "", elist.New("psql_expr_fn::Build: v.(psql_expr_node)")
        }
        arg, E = pnode.Build()
        if E != nil {
            return "", elist.Push(E, "psql_expr_fn::Build: pnode.Build()")
        }
        // Parentheses are never needed for arguments of functions.
        //        result += "(" + arg + ")";
        result += arg
    }
    result += ")"
    return result, nil
}   // End of function psql_expr_fn::Build.

//=============================================================================
//=============================================================================

/*
This is the expression-object which the user of this package will receive.
A Psql_expr object is a PostgreSQL expression or sub-expression.
The member "node" is an opaque handle.
So a Pqsl_expr object may be freely copied.

The error field E is public so that users can inspect it.
*/
type Psql_expr struct {
    //----------------------//
    //      Psql_expr::     //
    //----------------------//
    /*------------------------------------------------------------------------------
      NOTE: Maybe E should be private to prevent modification??
      The error is always set to the same as the return value of a function.
      So E is only useful to inform functions whose arguments are outputs
      from Xstr, Xnum, Xfld, Xop and Xfn of the error stack which has occurred.
      In other words, the E field is a copy (or shadow) of the last error
      returned by a member function of Psql_expr::.
      ------------------------------------------------------------------------------*/
    node psql_expr_node // Union of *psql_expr_str/num/field/op/fn.
    E    error          // A copy of the error returned by functions.
}

func (p *Psql_expr) setNode(node psql_expr_node) error {
    //----------------------//
    //  Psql_expr::setNode  //
    //----------------------//
    if p == nil {
        return elist.New("Psql_expr::setNode: p == nil")
    }
    // Don't allow re-set of a non-nil value.
    if p.node != nil {
        p.E = elist.New("Psql_expr::setNode: p.node == nil")
        return p.E
    }
    // Don't allow setting to a nil value.
    if node == nil {
        p.E = elist.New("Psql_expr::setNode: node == nil")
        return p.E
    }
    p.node = node
    p.E = nil
    return nil
}   // End of function Psql_expr::setNode.

func (p *Psql_expr) getNode() psql_expr_node {
    //----------------------//
    //  Psql_expr::getNode  //
    //----------------------//
    if p == nil {
        return nil
    }
    return p.node
}   // End of function Psql_expr::getNode.

/*
This function creates a string object and points to it permanently.
*/
func (p *Psql_expr) SetStr(s string) error {
    //----------------------//
    //   Psql_expr::SetStr  //
    //----------------------//
    if p == nil {
        return elist.New("Psql_expr::SetStr: p == nil")
    }
    // Don't allow re-set of a non-nil value.
    if p.node != nil {
        p.E = elist.New("Psql_expr::SetStr: p.node == nil")
        return p.E
    }
    // Create a new string-object.
    var E error
    q := new(psql_expr_str)
    E = q.Set(s)
    if E != nil {
        p.E = elist.Push(E, "Psql_expr::SetStr: q.Set(s)")
        return p.E
    }
    // Point to the node-object.
    E = p.setNode(q)
    if E != nil {
        p.E = elist.Push(E, "Psql_expr::SetStr: p.setNode(q)")
        return p.E
    }
    p.E = nil
    return nil
}   // End of function Psql_expr::SetStr.

/*
This function creates a number-object and points to it permanently.
*/
func (p *Psql_expr) SetNum(v interface{}) error {
    //----------------------//
    //   Psql_expr::SetNum  //
    //----------------------//
    if p == nil {
        return elist.New("Psql_expr::SetNum: p == nil")
    }
    // Don't allow re-set of a non-nil value.
    if p.node != nil {
        p.E = elist.New("Psql_expr::SetNum: p.node == nil")
        return p.E
    }
    // Create a new number-object.
    var E error
    q := new(psql_expr_num)
    E = q.Set(v)
    if E != nil {
        p.E = elist.Push(E, "Psql_expr::SetNum: q.Set(v)")
        return p.E
    }
    // Point to the node-object.
    E = p.setNode(q)
    if E != nil {
        p.E = elist.Push(E, "Psql_expr::SetNum: setNode(q)")
        return p.E
    }
    p.E = nil
    return nil
}   // End of function Psql_expr::SetNum.

/*
This function creates a number-object and points to it permanently.
*/
func (p *Psql_expr) SetFld(tab string, fld string) error {
    //----------------------//
    //   Psql_expr::SetFld  //
    //----------------------//
    if p == nil {
        return elist.New("Psql_expr::SetFld: p == nil")
    }
    // Don't allow re-set of a non-nil value.
    if p.node != nil {
        p.E = elist.New("Psql_expr::SetFld: p.node == nil")
        return p.E
    }
    // Create a new field-object.
    var E error
    q := new(psql_expr_field)
    E = q.Set(tab, fld)
    if E != nil {
        p.E = elist.Push(E, "Psql_expr::SetFld: q.Set()")
        return p.E
    }
    // Point to the node-object.
    E = p.setNode(q)
    if E != nil {
        p.E = elist.Push(E, "Psql_expr::SetFld: p.setNode()")
        return p.E
    }
    p.E = nil
    return nil
}   // End of function Psql_expr::SetFld.

/*
This function creates a operation-object and points to it permanently.
*/
func (p *Psql_expr) SetOp(op string, lft *Psql_expr, rt *Psql_expr) error {
    //----------------------//
    //   Psql_expr::SetOp   //
    //----------------------//
    if p == nil {
        return elist.New("Psql_expr::SetOp: p == nil")
    }
    // Don't allow re-set of a non-nil value.
    if p.node != nil {
        p.E = elist.New("Psql_expr::SetOp: p.node == nil")
        return p.E
    }
    // Extract the expression nodes from the expressions.
    var lft_node psql_expr_node
    var rt_node psql_expr_node
    if lft != nil {
        // Check that this node does not already have a parent.
        if lft.node != nil && lft.node.getParent() != nil {
            return elist.New("Psql_expr::SetOp: lft.node.getParent() != nil")
        }
        lft_node = lft.getNode()
    }
    if rt != nil {
        // Check that this node does not already have a parent.
        if rt.node != nil && rt.node.getParent() != nil {
            return elist.New("Psql_expr::SetOp: rt.node.getParent() != nil")
        }
        rt_node = rt.getNode()
    }

    // Create a new operation-object.
    var E error
    q := new(psql_expr_op)
    E = q.Set(op, lft_node, rt_node)
    if E != nil {
        p.E = elist.Push(E, "Psql_expr::SetOp: q.Set()")
        return p.E
    }
    // Point to the node-object.
    E = p.setNode(q)
    if E != nil {
        p.E = elist.Push(E, "Psql_expr::SetOp: p.setNode()")
        return p.E
    }
    p.E = nil
    return nil
}   // End of function Psql_expr::SetOp.

/*
This function creates a function-object and points to it permanently.
*/
func (p *Psql_expr) SetFn(fn string, args ...*Psql_expr) error {
    //----------------------//
    //   Psql_expr::SetFn   //
    //----------------------//
    if p == nil {
        return elist.New("Psql_expr::SetFn: p == nil")
    }
    // Don't allow re-set of a non-nil value.
    if p.node != nil {
        p.E = elist.New("Psql_expr::SetFn: p.node == nil")
        return p.E
    }
    // Extract the expression nodes from the expressions.
    n_args := len(args)
    arg_nodes := make([]psql_expr_node, n_args)
    for i, arg := range args {
        // Nil arguments are forbidden.
        if arg == nil || arg.node == nil {
            return elist.New("Psql_expr::SetFn: arg == nil || arg.node == nil")
        }
        // Check that this node does not already have a parent.
        if arg.node.getParent() != nil {
            return elist.New("Psql_expr::SetFn: arg.node.getParent() != nil")
        }
        arg_nodes[i] = arg.getNode()
    }

    // Create a new function-object.
    var E error
    q := new(psql_expr_fn)

    // Set the argument nodes. This should set the parent-pointers.
    E = q.Set(fn, arg_nodes...)
    if E != nil {
        p.E = elist.Push(E, "Psql_expr::SetFn: q.Set()")
        return p.E
    }
    // Point to the node-object.
    E = p.setNode(q)
    if E != nil {
        p.E = elist.Push(E, "Psql_expr::SetFn: q.setNode()")
        return p.E
    }
    p.E = nil
    return nil
}   // End of function Psql_expr::SetFn.

/*
This function clones a copy of a given expression.
It probably isn't useful.
*/
func (p *Psql_expr) Clone() *Psql_expr {
    //----------------------//
    //   Psql_expr::Clone   //
    //----------------------//
    if p == nil {
        return nil
    }
    // Create a new expression.
    q := new(Psql_expr)

    // This should never happen. Very bad!
    if p.node == nil {
        q.E = elist.New("Psql_expr::Clone: p.node == nil")
        return q
    }

    // The following clone-calls should set the parent pointers.
    switch x := p.node.(type) {
    case *psql_expr_str:
        q.node = x.clone()
    case *psql_expr_num:
        q.node = x.clone()
    case *psql_expr_field:
        q.node = x.clone()
    case *psql_expr_op:
        q.node = x.clone()
    case *psql_expr_fn:
        q.node = x.clone()
    default:
        return nil
    }
    return q
}   // End of function Psql_expr::Clone.

func (p *Psql_expr) Build() (string, error) {
    //----------------------//
    //   Psql_expr::Build   //
    //----------------------//
    /*------------------------------------------------------------------------------
      For the Postgres standard for string escape characters, see:
      https://www.postgresql.org/docs/8.0/static/sql-syntax.html#SQL-SYNTAX-CONSTANTS
      - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
      This function does not update p.E because the function only reads p.
      ------------------------------------------------------------------------------*/
    if p == nil {
        return "", elist.New("Psql_expr::Build: p == nil")
    }
    // Nil expressions are not permitted
    if p.node == nil {
        return "", elist.Push(p.E, "Psql_expr::Build: p.node == nil")
    }
    var str string = ""
    var E error

    // Build the string.
    str, E = p.node.Build()
    if E != nil {
        return "", elist.Push(E, "Psql_expr::Build: p.node.Build()")
    }
    return str, nil
}   // End of function Psql_expr::Build.

//=============================================================================
//=============================================================================
/*------------------------------------------------------------------------------
Some non-method functions to construct new Psql_expr objects with.
------------------------------------------------------------------------------*/

/*
Create a leaf-node for an SQL string.
*/
func Xstr(s string) *Psql_expr {
    //----------------------//
    //         Xstr         //
    //----------------------//
    p := new(Psql_expr)
    E := p.SetStr(s)
    if E != nil {
        p.E = elist.New("Xstr: p.SetStr(s)")
    }
    return p
}   // End of function Xstr.

/*
Create a leaf-node for an SQL number.
Argument can be int, float, or a string representing a number.
*/
func Xnum(v interface{}) *Psql_expr {
    //----------------------//
    //         Xnum         //
    //----------------------//
    p := new(Psql_expr)
    E := p.SetNum(v)
    if E != nil {
        p.E = elist.New("Xnum: p.SetNum(v)")
    }
    return p
}   // End of function Xnum.

/*
Create a leaf-node for an SQL table-field (i.e. a column).
Arguments can be:
    "", field       where field != ""
    table, field    where table != "" and field != ""
*/
func Xfld(tab string, fld string) *Psql_expr {
    //----------------------//
    //         Xfld         //
    //----------------------//
    p := new(Psql_expr)
    E := p.SetFld(tab, fld)
    if E != nil {
        p.E = elist.New("Xfld: p.SetFld(tab, fld)")
    }
    return p
}   // End of function Xfld.

/*
Create an internal node for an SQL prefix or infix operator-expression.
Arguments can be:
    op, nil, rt     where rt != nil (prefix expression)
    op, lft, rt     where lft != nil and rt != nil (infix expression)
*/
func Xop(op string, lft *Psql_expr, rt *Psql_expr) *Psql_expr {
    //----------------------//
    //          Xop         //
    //----------------------//
    p := new(Psql_expr)
    E := p.SetOp(op, lft, rt)
    if E != nil {
        p.E = elist.New("Xop: p.SetOp(op, lft, rt)")
    }
    return p
}   // End of function Xop.

/*
Create an internal node for an SQL function with arguments.
*/
func Xfn(fn string, args ...*Psql_expr) *Psql_expr {
    //----------------------//
    //          Xfn         //
    //----------------------//
    p := new(Psql_expr)
    E := p.SetFn(fn, args...)
    if E != nil {
        p.E = elist.New("Xfn: p.SetFn(fn, args)")
    }
    return p
}   // End of function Xfn.

//=============================================================================
//=============================================================================

/*------------------------------------------------------------------------------
A psql_expr_as is a PostgreSQL expression with an optional AS-clause.
------------------------------------------------------------------------------*/
type psql_expr_as struct {
    //----------------------//
    //    psql_expr_as::    //
    //----------------------//
    expr psql_expr_node // Expression-node interface-object.
    as   string         // Output name. The column name in the output.
}

/*------------------------------------------------------------------------------
NOTE: Must check that an expression cannot be used twice here?
------------------------------------------------------------------------------*/
func (p *psql_expr_as) Set(expr psql_expr_node, as string) error {
    //----------------------//
    //   psql_expr_as::Set  //
    //----------------------//
    if p == nil {
        return elist.New("psql_expr_as::Set: p == nil")
    }
    p.expr = expr
    p.as = as
    return nil
}   // End of function psql_expr_as::Set.

/*------------------------------------------------------------------------------
For the Postgres standard for string escape characters, see:
https://www.postgresql.org/docs/8.0/static/sql-syntax.html#SQL-SYNTAX-CONSTANTS
Note that this function only builds the expression-string, not the as-string.
For building the as-string, see Psql_select::SelectBuild.
------------------------------------------------------------------------------*/
func (p *psql_expr_as) Build() (string, error) {
    //--------------------------//
    //    psql_expr_as::Build   //
    //--------------------------//
    if p == nil {
        return "", elist.New("psql_expr_as::Build: p == nil")
    }
    if p.expr == nil {
        return "", elist.New("psql_expr_as::Build: p.expr == nil")
    }
    var str string = ""
    var E error

    // Build the string.
    str, E = p.expr.Build()
    if E != nil {
        return "", elist.Push(E, "psql_expr_as::Build: p.expr.Build()")
    }
    return str, nil
}   // End of function psql_expr_as::Build.

//=============================================================================
//=============================================================================

/*------------------------------------------------------------------------------
Common interface for all expression tree-nodes.
Union of *psql_from_table/select/fn1/fn2/join.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
A psql_from_item is a PostgreSQL from-item.
https://www.postgresql.org/docs/8.0/static/sql-select.html#SQL-FROM
1. table name/alias
2. sub-select clause with mandatory alias
3. function
4. function
5. joined from-items
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Options 1 and 5 are most needed initially.
Option 5 can be a general binary tree of joins!
Option 2 refers to a full Psql_select object!!!
------------------------------------------------------------------------------*/
type psql_from_node interface {
    //----------------------//
    //   psql_from_node{}   //
    //----------------------//
    getParent() psql_from_node
    setParent(psql_from_node)
    Build() (string, error)
}

//=============================================================================
//=============================================================================

/*------------------------------------------------------------------------------
A table-name with optional alias.
https://www.postgresql.org/docs/8.0/static/sql-select.html
------------------------------------------------------------------------------*/
type psql_from_table struct {
    //----------------------//
    //   psql_from_table::  //
    //----------------------//
    only   bool   // True means do not scan descendants.
    table  string // Table name.
    alias  string
    parent psql_from_node
}

func (p *psql_from_table) getParent() psql_from_node {
    //------------------------------//
    //  psql_from_table::getParent  //
    //------------------------------//
    if p == nil {
        return nil
    }
    return p.parent
}   // End of function psql_from_table::getParent.

func (p *psql_from_table) setParent(q psql_from_node) {
    //------------------------------//
    //  psql_from_table::setParent  //
    //------------------------------//
    if p == nil {
        return
    }
    p.parent = q
}   // End of function psql_from_table::setParent.

func (p *psql_from_table) Set(table string, alias string) error {
    //----------------------//
    // psql_from_table::Set //
    //----------------------//
    if p == nil {
        return elist.New("psql_expr_table::Set: p == nil")
    }
    if table == "" {
        return elist.New("psql_expr_table::Set: table == nil")
    }
    p.only = false
    p.table = table
    p.alias = alias
    return nil
}   // End of function psql_from_table::Set.

/*------------------------------------------------------------------------------
This version of the from-table set-method precedes the output with "ONLY ".
This prevents descendants of the table from being scanned.
This will probably be required only very rarely.
------------------------------------------------------------------------------*/
func (p *psql_from_table) SetOnly(table string, alias string) error {
    //--------------------------//
    // psql_from_table::SetOnly //
    //--------------------------//
    if p == nil {
        return elist.New("psql_expr_table::SetOnly: p == nil")
    }
    if table == "" {
        return elist.New("psql_expr_table::SetOnly: table == nil")
    }
    p.only = true
    p.table = table
    p.alias = alias
    return nil
}   // End of function psql_from_table::Set.

/*------------------------------------------------------------------------------
For the Postgres standard for string escape characters, see:
https://www.postgresql.org/docs/8.0/static/sql-syntax.html#SQL-SYNTAX-CONSTANTS
------------------------------------------------------------------------------*/
func (p *psql_from_table) Build() (string, error) {
    //--------------------------//
    //  psql_from_table::Build  //
    //--------------------------//
    if p == nil {
        return "", elist.New("psql_expr_table::Build: p == nil")
    }
    if p.table == "" {
        return "", elist.New("psql_expr_table::Build: p.table == nil")
    }
    // The rule for PostgreSQL identifiers is that double-quote is repeated.
    var rep = strings.NewReplacer(`"`, `""`)

    var only, table, alias string
    if p.only {
        only = "ONLY "
    }
    table = fmt.Sprintf("\"%s\"", rep.Replace(p.table))
    if p.alias != "" {
        // The word "AS" is optional.
        alias = fmt.Sprintf(" AS \"%s\"", rep.Replace(p.alias))
    }
    return only + table + alias, nil
}   // End of function psql_from_table::Build.

//=============================================================================
//=============================================================================

/*------------------------------------------------------------------------------
A psql_from_item is a PostgreSQL points to a psql_from_node.
The only benefit of this is that two separate objects in the from-list
of a Psql_select:: object can point to the same psql_from_node.
The field "node" is an interface which is a union of pointer-types.
------------------------------------------------------------------------------*/
type psql_from_item struct {
    //----------------------//
    //   psql_from_item::   //
    //----------------------//
    node psql_from_node // From-item-node interface.
}

/*------------------------------------------------------------------------------
NOTE: Must check that a from-node cannot be pointed to twice here?
------------------------------------------------------------------------------*/
func (p *psql_from_item) Set(node psql_from_node) error {
    //----------------------//
    //  psql_from_item::Set //
    //----------------------//
    if p == nil {
        return elist.New("psql_from_item::Set: p == nil")
    }
    p.node = node
    return nil
}   // End of function psql_from_item::Set.

/*------------------------------------------------------------------------------
For the Postgres standard for string escape characters, see:
https://www.postgresql.org/docs/8.0/static/sql-syntax.html#SQL-SYNTAX-CONSTANTS
------------------------------------------------------------------------------*/
func (p *psql_from_item) Build() (string, error) {
    //--------------------------//
    //   psql_from_item::Build  //
    //--------------------------//
    if p == nil {
        return "", elist.New("psql_from_item::Build: p == nil")
    }
    if p.node == nil {
        return "", elist.New("psql_from_item::Build: p.node == nil")
    }
    var E error
    var str string = ""

    // Build the string.
    str, E = p.node.Build()
    if E != nil {
        return "", elist.Push(E, "psql_from_item::Build: p.node.Build()")
    }
    return str, nil
}   // End of function psql_from_item::Build.

//=============================================================================
//=============================================================================

/*
Psql_from is the class of from-object which users of this package will use.
A Psql_from object represents a PostgreSQL from-item.
*/
type Psql_from struct {
    //----------------------//
    //      Psql_from::     //
    //----------------------//
    /*------------------------------------------------------------------------------
      The member "node" is an opaque handle containing only a pointer.
      So a Pqsl_from object may be freely copied.
      ------------------------------------------------------------------------------*/
    node psql_from_node // Union of *psql_from_table/select/fn1/fn2/join.
}

func (p *Psql_from) setNode(node psql_from_node) error {
    //----------------------//
    //  Psql_from::setNode  //
    //----------------------//
    if p == nil {
        return elist.New("Psql_from::setNode: p == nil")
    }
    // Don't allow re-set of a non-nil value.
    if p.node != nil {
        return elist.New("Psql_from::setNode: p.node == nil")
    }
    // Don't allow setting to a nil value.
    if node == nil {
        return elist.New("Psql_from::setNode: node == nil")
    }
    p.node = node
    return nil
}   // End of function Psql_from::setNode.

func (p *Psql_from) SetTable(table string, alias string) error {
    //----------------------//
    //  Psql_from::SetTable //
    //----------------------//
    if p == nil {
        return elist.New("Psql_from::SetTable: p == nil")
    }
    // Don't allow re-set of a non-nil value.
    if p.node != nil {
        return elist.New("Psql_from::SetTable: p.node == nil")
    }
    // Create a new from-object.
    var E error
    q := new(psql_from_table)
    E = q.Set(table, alias)
    if E != nil {
        return elist.Push(E, "Psql_from::SetTable: q.Set()")
    }
    // Point to the node-object.
    E = p.setNode(q)
    if E != nil {
        return elist.Push(E, "Psql_from::SetTable: p.setNode()")
    }
    return nil
}   // End of function Psql_from::SetTable.

//=============================================================================
//=============================================================================

/*
Create a leaf-node for an SQL table-field (i.e. a column).
Arguments can be:
    "", field       where field != ""
    table, field    where table != "" and field != ""
*/
func Xtable(table string, alias string) *Psql_from {
    //----------------------//
    //        Xtable        //
    //----------------------//
    p := new(Psql_from)
    E := p.SetTable(table, alias)
    if E != nil {
        return nil
    }
    return p
}   // End of function Xtable.

//=============================================================================
//=============================================================================

/*------------------------------------------------------------------------------
A psql_order is a PostgreSQL expression with an optional ASC/DESC.
NOTE: Could also add a USING clause.
https://www.postgresql.org/docs/8.0/static/sql-select.html#SQL-ORDERBY
------------------------------------------------------------------------------*/
type psql_order struct {
    //----------------------//
    //     psql_order::     //
    //----------------------//
    expr psql_expr_node // Expression-node interface-object.
    desc bool           // Descending. Default is ascending.
}

/*------------------------------------------------------------------------------
Specified order.
NOTE: Must check that an expression cannot be used twice here?
------------------------------------------------------------------------------*/
func (p *psql_order) SetDirn(expr psql_expr_node, dirn bool) error {
    //----------------------//
    //  psql_order::SetDirn //
    //----------------------//
    if p == nil {
        return elist.New("psql_order::SetDirn: p == nil")
    }
    p.expr = expr
    p.desc = dirn
    return nil
}   // End of function psql_order::SetDirb.

/*------------------------------------------------------------------------------
Ascending order.
------------------------------------------------------------------------------*/
func (p *psql_order) Set(expr psql_expr_node) error {
    //----------------------//
    //    psql_order::Set   //
    //----------------------//
    if p == nil {
        return elist.New("psql_order::Set: p == nil")
    }
    return p.SetDirn(expr, false)
}   // End of function psql_order::Set.

/*------------------------------------------------------------------------------
Descending order.
------------------------------------------------------------------------------*/
func (p *psql_order) SetDesc(expr psql_expr_node) error {
    //----------------------//
    //  psql_order::SetDesc //
    //----------------------//
    if p == nil {
        return elist.New("psql_order::SetDesc: p == nil")
    }
    return p.SetDirn(expr, true)
}   // End of function psql_order::SetDesc.

func (p *psql_order) Build() (string, error) {
    //----------------------//
    //   psql_order::Build  //
    //----------------------//
    if p == nil {
        return "", elist.New("psql_order::Build: p == nil")
    }
    if p.expr == nil {
        return "", elist.New("psql_order::Build: p.expr == nil")
    }
    var E error
    var str string

    // Build the string.
    str, E = p.expr.Build()
    if E != nil {
        return "", elist.Push(E, "psql_order::Build: p.expr.Build()")
    }
    if p.desc {
        str += " DESC"
    }
    return str, nil
}   // End of function psql_order::Build.

//=============================================================================
//=============================================================================

/*------------------------------------------------------------------------------
A psql_limit is a PostgreSQL LIMIT-clause with optional OFFSET.
https://www.postgresql.org/docs/8.0/static/sql-select.html#SQL-LIMIT
The field "limit" and "offset" represent:
limit = 0       No limit.
limit > 0       Limit equals "limit" rows.
offset = 0      No offset. Same as offset = 0.
offset > 0      Offset equals "offset" rows.
------------------------------------------------------------------------------*/
type psql_limit struct {
    //----------------------//
    //     psql_limit::     //
    //----------------------//
    limit  uint64 // Limit on the number of rows to fetch.
    offset uint64 // Offset for the block of rows.
}

/*------------------------------------------------------------------------------
Set both the limit and the offset.
Unfortunately Go does not allow default arguments!
------------------------------------------------------------------------------*/
func (p *psql_limit) Set(limit uint64, offset uint64) error {
    //----------------------//
    //    psql_limit::Set   //
    //----------------------//
    if p == nil {
        return elist.New("psql_limit::Set: p == nil")
    }
    p.limit = limit
    p.offset = offset
    return nil
}   // End of function psql_limit::Set.

func (p *psql_limit) Build() (string, error) {
    //----------------------//
    //   psql_limit::Build  //
    //----------------------//
    if p == nil {
        return "", elist.New("psql_limit::Build: p == nil")
    }
    var str string
    if p.limit > 0 {
        str += fmt.Sprintf(" LIMIT %d", p.limit)
    }
    if p.offset > 0 {
        str += fmt.Sprintf(" OFFSET %d", p.offset)
    }
    return str, nil
}   // End of function psql_limit::Build.

//=============================================================================
//=============================================================================

type Psql_select struct {
    //----------------------//
    //     Psql_select::    //
    //----------------------//
    /*------------------------------------------------------------------------------
      NOTE: Class Psql_select:: must be moved to a private class psql_select_cmd::
      which is pointed to by a private pointer-field of class Psql_select::.
          The field "sel" is a list of psql_expr_as:: objects.
          The field "from" is a list of psql_from_item:: objects.
          The field "where" is a psql_expr_node:: interface-object.
          The field "orderby" is a list of psql_order:: objects.
          The field "limit" is a psql_limit:: object.
      ------------------------------------------------------------------------------*/
    sel     List_base      // List of expressions to select.
    from    List_base      // List of tables to select from.
    where   psql_expr_node // Single boolean constraint on fields.
    orderby List_base      // Ordering rules.
    limit   psql_limit     // Limit on number of lines of output.
}

func (p *Psql_select) Clear() error {
    //----------------------//
    //  Psql_select::Clear  //
    //----------------------//
    if p == nil {
        return elist.New("Psql_select::Clear: p == nil")
    }
    var E error
    E = p.sel.Clear()
    if E != nil {
        return elist.Push(E, "Psql_select::Clear: p.sel.Clear()")
    }
    E = p.from.Clear()
    if E != nil {
        return elist.Push(E, "Psql_select::Clear: p.from.Clear()")
    }
    p.where = nil

    E = p.orderby.Clear()
    if E != nil {
        return elist.Push(E, "Psql_select::Clear: p.orderby.Clear()")
    }
    return nil
}   // End of function Psql_select::Clear.

func (p *Psql_select) SelectAppendStrAs(s string, as string) error {
    //----------------------------------//
    //  Psql_select::SelectAppendStrAs  //
    //----------------------------------//
    if p == nil {
        return elist.New("Psql_select::SelectAppendStrAs: p == nil")
    }
    var E error

    str := new(psql_expr_str)
    E = str.Set(s)
    if E != nil {
        return elist.Push(E, "Psql_select::SelectAppendStrAs: str.Set(s)")
    }

    expr_as := new(psql_expr_as)
    E = expr_as.Set(str, as)
    if E != nil {
        return elist.Push(E, "Psql_select::SelectAppendStrAs: expr_as.Set()")
    }

    E = p.sel.AppendValue(expr_as)
    if E != nil {
        return elist.Push(E,
            "Psql_select::SelectAppendStrAs: p.sel.AppendValue()")
    }
    return nil
}   // End of function Psql_select::SelectAppendStrAs.

func (p *Psql_select) SelectAppendStr(s string) error {
    //------------------------------//
    // Psql_select::SelectAppendStr //
    //------------------------------//
    if p == nil {
        return elist.New("Psql_select::SelectAppendStr: p == nil")
    }
    return p.SelectAppendStrAs(s, "")
}   // End of function Psql_select::SelectAppendStr.

/*
Append a field to the select field-list.
The argument "v" may be an integer, float64 or a string which represents
an integer or floating-point constant.
*/
func (p *Psql_select) SelectAppendNumAs(v interface{}, as string) error {
    //----------------------------------//
    //  Psql_select::SelectAppendNumAs  //
    //----------------------------------//
    /*------------------------------------------------------------------------------
      For PostgreSQL numeric constants, see
      https://www.postgresql.org/docs/8.0/static/sql-syntax.html#SQL-SYNTAX-CONSTANTS
      See section 4.1.2.4 "Numeric constants".
          digits
          digits.[digits][e[+-]digits]
          [digits].digits[e[+-]digits]
          digits e[+-]digits
      ------------------------------------------------------------------------------*/
    if p == nil {
        return elist.New("Psql_select::SelectAppendNumAs: p == nil")
    }
    var E error
    num := new(psql_expr_num)
    E = num.Set(v)
    if E != nil {
        return elist.Push(E, "Psql_select::SelectAppendNumAs: num.Set(v)")
    }

    expr_as := new(psql_expr_as)
    E = expr_as.Set(num, as)
    if E != nil {
        return elist.Push(E, "Psql_select::SelectAppendNumAs: expr_as.Set()")
    }

    E = p.sel.AppendValue(expr_as)
    if E != nil {
        return elist.Push(E,
            "Psql_select::SelectAppendNumAs: p.sel.AppendValue(expr_as)")
    }
    return nil
}   // End of function Psql_select::SelectAppendNumAs.

func (p *Psql_select) SelectAppendNum(v interface{}) error {
    //------------------------------//
    // Psql_select::SelectAppendNum //
    //------------------------------//
    if p == nil {
        return elist.New("Psql_select::SelectAppendNum: p == nil")
    }
    return p.SelectAppendNumAs(v, "")
}   // End of function Psql_select::SelectAppendNum.

func (p *Psql_select) SelectAppendFldAs(tab string,
    //----------------------------------//
    //  Psql_select::SelectAppendFldAs  //
    //----------------------------------//
    fld string, as string) error {
    if p == nil {
        return elist.New("Psql_select::SelectAppendFldAs: p == nil")
    }
    var E error

    field := new(psql_expr_field)
    E = field.Set(tab, fld)
    if E != nil {
        return elist.Push(E, "Psql_select::SelectAppendFldAs: field.Set()")
    }

    expr_as := new(psql_expr_as)
    E = expr_as.Set(field, as)
    if E != nil {
        return elist.Push(E, "Psql_select::SelectAppendFldAs: expr_as.Set()")
    }

    E = p.sel.AppendValue(expr_as)
    if E != nil {
        return elist.Push(E,
            "Psql_select::SelectAppendFldAs: p.sel.AppendValue(expr_as)")
    }
    return nil
}   // End of function Psql_select::SelectAppendFldAs.

func (p *Psql_select) SelectAppendFld(tab string, fld string) error {
    //------------------------------//
    // Psql_select::SelectAppendFld //
    //------------------------------//
    if p == nil {
        return elist.New("Psql_select::SelectAppendFld: p == nil")
    }
    return p.SelectAppendFldAs(tab, fld, "")
}   // End of function Psql_select::SelectAppendFld.

func (p *Psql_select) SelectAppendOpAs(expr_op *psql_expr_op,
    //----------------------------------//
    //   Psql_select::SelectAppendOpAs  //
    //----------------------------------//
    as string) error {
    if p == nil {
        return elist.New("Psql_select::SelectAppendOpAs: p == nil")
    }
    if expr_op == nil {
        return elist.New("Psql_select::SelectAppendOpAs: expr_op == nil")
    }
    var E error

    expr_as := new(psql_expr_as)
    E = expr_as.Set(expr_op, as)
    if E != nil {
        return elist.Push(E,
            "Psql_select::SelectAppendOpAs: expr_as.Set(expr_op, as)")
    }
    E = p.sel.AppendValue(expr_as)
    if E != nil {
        return elist.Push(E,
            "Psql_select::SelectAppendOpAs: p.sel.AppendValue(expr_as)")
    }
    return nil
}   // End of function Psql_select::SelectAppendOpAs.

func (p *Psql_select) SelectAppendOp(expr_op *psql_expr_op) error {
    //------------------------------//
    //  Psql_select::SelectAppendOp //
    //------------------------------//
    if p == nil {
        return elist.New("Psql_select::SelectAppendOp: p == nil")
    }
    return p.SelectAppendOpAs(expr_op, "")
}   // End of function Psql_select::SelectAppendOp.

func (p *Psql_select) SelectAppendFnAs(expr_fn *psql_expr_fn, as string) error {
    //----------------------------------//
    //   Psql_select::SelectAppendFnAs  //
    //----------------------------------//
    if p == nil {
        return elist.New("Psql_select::SelectAppendFnAs: p == nil")
    }
    var E error

    expr_as := new(psql_expr_as)
    E = expr_as.Set(expr_fn, as)
    if E != nil {
        return elist.Push(E, "Psql_select::SelectAppendFnAs: expr_as.Set()")
    }
    E = p.sel.AppendValue(expr_as)
    if E != nil {
        return elist.Push(E,
            "Psql_select::SelectAppendFnAs: p.sel.AppendValue")
    }
    return nil
}   // End of function Psql_select::SelectAppendFnAs.

func (p *Psql_select) SelectAppendFn(expr_fn *psql_expr_fn) error {
    //------------------------------//
    //  Psql_select::SelectAppendFn //
    //------------------------------//
    if p == nil {
        return elist.New("Psql_select::SelectAppendFn: p == nil")
    }
    return p.SelectAppendFnAs(expr_fn, "")
}   // End of function Psql_select::SelectAppendFn.

func (p *Psql_select) SelectAppendExprAs(expr *Psql_expr, as string) error {
    //----------------------------------//
    //  Psql_select::SelectAppendExprAs //
    //----------------------------------//
    if p == nil {
        return elist.New("Psql_select::SelectAppendExprAs: p == nil")
    }
    if expr == nil {
        return elist.New("Psql_select::SelectAppendExprAs: expr == nil")
    }
    if expr.E != nil {
        return elist.Push(expr.E,
            "Psql_select::SelectAppendExprAs: expr.E != nil")
    }
    if expr.node == nil {
        return elist.New("Psql_select::SelectAppendExprAs: expr.node == nil")
    }
    var E error

    expr_as := new(psql_expr_as)
    // Should check that the same node is not used twice!
    E = expr_as.Set(expr.node, as)
    if E != nil {
        return elist.Push(E, "Psql_select::SelectAppendExprAs: expr_as.Set()")
    }
    E = p.sel.AppendValue(expr_as)
    if E != nil {
        return elist.Push(E,
            "Psql_select::SelectAppendExprAs: p.sel.AppendValue")
    }
    return nil
}   // End of function Psql_select::SelectAppendExprAs.

func (p *Psql_select) SelectAppendExpr(expr *Psql_expr) error {
    //------------------------------//
    // Psql_select::SelectAppendExpr//
    //------------------------------//
    if p == nil {
        return elist.New("Psql_select::SelectAppendExpr: p == nil")
    }
    return p.SelectAppendExprAs(expr, "")
}   // End of function Psql_select::SelectAppendExpr.

func (p *Psql_select) SelectAppendExprs(exprs ...*Psql_expr) error {
    //----------------------------------//
    //  Psql_select::SelectAppendExprs  //
    //----------------------------------//
    if p == nil {
        return elist.New("Psql_select::SelectAppendExprs: p == nil")
    }
    // Nothing to do for the empty list.
    if exprs == nil || len(exprs) == 0 {
        return nil
    }
    // First pass. Check for some basic errors.
    for i, expr := range exprs {
        if expr == nil {
            return elist.Newf("SelectAppendExprs: argument[%d] == nil", i)
        }
        if expr.E != nil {
            return elist.Pushf(expr.E,
                "SelectAppendExprs: argument[%d].E != nil", i)
        }
        if expr.node == nil {
            return elist.Newf(
                "SelectAppendExprs: argument[%d].node == nil", i)
        }
    }

    // Second pass. Append each expression to the select-list.
    var E error
    for i, expr := range exprs {
        E = p.SelectAppendExpr(expr)
        if E != nil {
            return elist.Pushf(E,
                "SelectAppendExprs: SelectAppendExpr(argument[%d])", i)
        }
    }
    return nil
}   // End of function Psql_select::SelectAppendExprs.

func (p *Psql_select) SelectBuild() (string, error) {
    //--------------------------//
    // Psql_select::SelectBuild //
    //--------------------------//
    if p == nil {
        return "", elist.New("Psql_select::SelectBuild: p == nil")
    }
    if p.sel.Empty() {
        return "", elist.New("Psql_select::SelectBuild: p.sel.Empty()")
    }
    var E error
    var res string
    var iter List_iter
    var curr *List_node

    E = iter.Init(&p.sel)
    if E != nil {
        return "", elist.Push(E, "Psql_select::SelectBuild: iter.Init()")
    }
    // The rule for PostgreSQL identifiers is that double-quote is repeated.
    var rep = strings.NewReplacer(`"`, `""`)

    // Build the expression list.
    var nloop int = 0
    var n_errors int = 0
    for curr, E = iter.Next(); curr != nil; curr, E = iter.Next() {
        if E != nil {
            return "", elist.Push(E, "Psql_select::SelectBuild: iter.Next()")
        }
        if nloop > 0 {
            res += ", "
        }
        nloop += 1
        var v interface{}
        v, E = curr.GetValue()
        if E != nil {
            return "", elist.Push(E,
                "Psql_select::SelectBuild: curr.GetValue()")
        }
        var expr_as, ok = v.(*psql_expr_as)
        var itm = ""
        // NOTE. Should really make a fuss if "ok" is false.
        if ok {
            itm, E = expr_as.Build()
            if E != nil {
                return "", elist.Push(E,
                    "Psql_select::SelectBuild: expr_as.Build()")
            }
            if expr_as.as != "" {
                itm += fmt.Sprintf(" AS \"%s\"", rep.Replace(expr_as.as))
            }
        } else {
            // Try to make this expression impossible for SQL queries.
            itm = fmt.Sprintf("error<<value: %v>>", v)
            n_errors += 1
        }
        res += itm
    }
    if n_errors > 0 {
        return res, elist.Newf(
            "Psql_select::SelectBuild: n_errors = %d", n_errors)
    }
    return res, nil
}   // End of function Psql_select::SelectBuild.

/*
Psql_select::FromAppendItem appends a user-built Psql_from object to the
Psql_select object's from-list.
*/
func (p *Psql_select) FromAppendItem(from *Psql_from) error {
    //------------------------------//
    //  Psql_select::FromAppendItem //
    //------------------------------//
    /*------------------------------------------------------------------------------
      Psql_from       User-built structure with opaque pointer to psql_from_node.
      psql_from_node  Private root node representing a from-expression.
      psql_from_item  Private from-item structure for the Psql_select from-list.
      ------------------------------------------------------------------------------*/
    if p == nil {
        return elist.New("Psql_select::FromAppendItem: p == nil")
    }
    if from == nil {
        return elist.New("Psql_select::FromAppendItem: from == nil")
    }
    if from.node == nil {
        return elist.New("Psql_select::FromAppendItem: from.node == nil")
    }
    var E error

    from_item := new(psql_from_item)
    // Should check that the same node is not used twice!
    E = from_item.Set(from.node)
    if E != nil {
        return elist.Push(E,
            "Psql_select::FromAppendItem: from_item.Set(from.node)")
    }
    E = p.from.AppendValue(from_item)
    if E != nil {
        return elist.Push(E,
            "Psql_select::FromAppendItem: p.from.AppendValue(from_item)")
    }
    return nil
}   // End of function Psql_select::FromAppendItem.

func (p *Psql_select) FromBuild() (string, error) {
    //--------------------------//
    //  Psql_select::FromBuild  //
    //--------------------------//
    if p == nil {
        return "", elist.New("Psql_select::FromBuild: p == nil")
    }
    // Superfluous check.
    if p.sel.Empty() {
        return "", elist.New("Psql_select::FromBuild: p.sel.Empty()")
    }
    var E error
    var res string
    var iter List_iter
    var curr *List_node

    E = iter.Init(&p.from)
    if E != nil {
        return "", elist.Push(E, "Psql_select::FromBuild: iter.Init()")
    }
    // Build the expression list.
    var nloop int = 0
    var n_errors int = 0
    for curr, E = iter.Next(); curr != nil; curr, E = iter.Next() {
        if E != nil {
            return "", elist.Push(E, "Psql_select::FromBuild: iter.Next()")
        }
        if nloop > 0 {
            res += ", "
        }
        nloop += 1
        var v interface{}
        v, E = curr.GetValue()
        if E != nil {
            return "", elist.Push(E,
                "Psql_select::FromBuild: curr.GetValue()")
        }
        var from_item, ok = v.(*psql_from_item)
        var itm = ""
        // NOTE. Should really make a fuss if "ok" is false.
        if ok {
            itm, E = from_item.Build()
            if E != nil {
                return "", elist.Push(E,
                    "Psql_select::FromBuild: from_item.Build()")
            }
        } else {
            // Try to make this expression impossible for SQL queries.
            itm = fmt.Sprintf("error<<value: %v>>", v)
            n_errors += 1
        }
        res += itm
    }
    if n_errors > 0 {
        return res, elist.Newf(
            "Psql_select::FromBuild: n_errors = %d", n_errors)
    }
    return res, nil
}   // End of function Psql_select::FromBuild.

func (p *Psql_select) WhereSetExpr(where *Psql_expr) error {
    //------------------------------//
    //   Psql_select::WhereSetExpr  //
    //------------------------------//
    if p == nil {
        return elist.New("Psql_select::WhereSetExpr: p == nil")
    }
    if where == nil {
        return elist.New("Psql_select::WhereSetExpr: where == nil")
    }
    if where.E != nil {
        return elist.Push(where.E,
            "Psql_select::WhereSetExpr: where.E != nil")
    }
    if where.node == nil {
        return elist.New("Psql_select::WhereSetExpr: where.node == nil")
    }
    p.where = where.node
    return nil
}   // End of function Psql_select::WhereSetExpr.

/*
Specified direction.
    dirn = false:   ascending
    dirn = true:    descending

Psql_select::OrderAppend appends a user-built Psql_expr object to the
Psql_select object's "orderby" list.
*/
func (p *Psql_select) OrderAppendDirn(order *Psql_expr, dirn bool) error {
    //------------------------------//
    // Psql_select::OrderAppendDirn //
    //------------------------------//
    /*------------------------------------------------------------------------------
      Psql_expr       User-built structure with opaque pointer to psql_expr_node.
      psql_expr_node  Private root node representing an order-expression.
      psql_order      Private order-item structure for the Psql_select order-list.
      ------------------------------------------------------------------------------*/
    if p == nil {
        return elist.New("Psql_select::OrderAppendDirn: p == nil")
    }
    if order == nil {
        return elist.New("Psql_select::OrderAppendDirn: order == nil")
    }
    if order.E != nil {
        return elist.Push(order.E,
            "Psql_select::OrderAppendDirn: order.E != nil")
    }
    if order.node == nil {
        return elist.New("Psql_select::OrderAppendDirn: order.node == nil")
    }
    var E error

    order_item := new(psql_order)
    // Should check that the same node is not used twice!
    E = order_item.SetDirn(order.node, dirn)
    if E != nil {
        return elist.Push(E,
            "Psql_select::OrderAppendDirn: order_item.SetDirn()")
    }
    E = p.orderby.AppendValue(order_item)
    if E != nil {
        return elist.Push(E,
            "Psql_select::OrderAppendDirn: p.orderby.AppendValue()")
    }
    return nil
}   // End of function Psql_select::OrderAppendDirn.

/*
Ascending order.
*/
func (p *Psql_select) OrderAppend(order *Psql_expr) error {
    //--------------------------//
    // Psql_select::OrderAppend //
    //--------------------------//
    if p == nil {
        return elist.New("Psql_select::OrderAppend: p == nil")
    }
    return p.OrderAppendDirn(order, false)
}   // End of function Psql_select::OrderAppend.

/*
Descending order.
*/
func (p *Psql_select) OrderAppendDesc(order *Psql_expr) error {
    //------------------------------//
    // Psql_select::OrderAppendDesc //
    //------------------------------//
    if p == nil {
        return elist.New("Psql_select::OrderAppendDesc: p == nil")
    }
    return p.OrderAppendDirn(order, true)
}   // End of function Psql_select::OrderAppendDesc.

func (p *Psql_select) OrderBuild() (string, error) {
    //--------------------------//
    //  Psql_select::OrderBuild //
    //--------------------------//
    if p == nil {
        return "", elist.New("Psql_select::OrderBuild: p == nil")
    }
    // Superfluous check.
    if p.sel.Empty() {
        return "", elist.New("Psql_select::OrderBuild: p.sel.Empty()")
    }
    var E error
    var res string
    var iter List_iter
    var curr *List_node

    E = iter.Init(&p.orderby)
    if E != nil {
        return "", elist.Push(E, "Psql_select::OrderBuild: iter.Init()")
    }
    // Build the expression list.
    var nloop int = 0
    var n_errors int = 0
    for curr, E = iter.Next(); curr != nil; curr, E = iter.Next() {
        if E != nil {
            return "", elist.Push(E, "Psql_select::OrderBuild: iter.Next()")
        }
        if nloop > 0 {
            res += ", "
        }
        nloop += 1
        var v interface{}
        v, E = curr.GetValue()
        if E != nil {
            return "", elist.Push(E,
                "Psql_select::OrderBuild: curr.GetValue()")
        }
        var order_item, ok = v.(*psql_order)
        var itm = ""
        // NOTE. Should really make a fuss if "ok" is false.
        if ok {
            itm, E = order_item.Build()
            if E != nil {
                return "", elist.Push(E,
                    "Psql_select::OrderBuild: order_item.Build()")
            }
        } else {
            // Try to make this expression impossible for SQL queries.
            itm = fmt.Sprintf("error<<value: %v>>", v)
            n_errors += 1
        }
        res += itm
    }
    if n_errors > 0 {
        return res, elist.Newf(
            "Psql_select::OrderBuild: n_errors = %d", n_errors)
    }
    return res, nil
}   // End of function Psql_select::OrderBuild.

func (p *Psql_select) LimitSet(limit uint64, offset uint64) error {
    //--------------------------//
    //   Psql_select::LimitSet  //
    //--------------------------//
    if p == nil {
        return elist.New("Psql_select::LimitSet: p == nil")
    }
    var E error
    E = p.limit.Set(limit, offset)
    if E != nil {
        return elist.Push(E, "Psql_select::LimitSet: p.limit.Set()")
    }
    return nil
}   // End of function Psql_select::LimitSet.

func (p *Psql_select) Build() (string, error) {
    //----------------------//
    //  Psql_select::Build  //
    //----------------------//
    if p == nil {
        return "", elist.New("Psql_select::Build: p == nil")
    }
    if p.sel.Empty() {
        return "", elist.New("Psql_select::Build: p.sel.Empty()")
    }
    var E error
    var res string
    var build0 string

    // Add the select-list.
    build0, E = p.SelectBuild()
    if E != nil {
        return "", elist.Push(E, "Psql_select::Build: p.SelectBuild()")
    }
    res += "SELECT " + build0

    // Add the from-list.
    if !p.from.Empty() {
        build0, E = p.FromBuild()
        if E != nil {
            return "", elist.Push(E, "Psql_select::Build: p.FromBuild()")
        }
        res += " FROM " + build0
    }

    // Add the where-expression.
    if p.where != nil {
        build0, E = p.where.Build()
        if E != nil {
            return "", elist.Push(E, "Psql_select::Build: p.where.Build()")
        }
        res += " WHERE " + build0
    }

    // Add the order-by list.
    if !p.orderby.Empty() {
        build0, E = p.OrderBuild()
        if E != nil {
            return "", elist.Push(E, "Psql_select::Build: p.OrderBuild()")
        }
        res += " ORDER BY " + build0
    }

    // Add the limit-offset-expression.
    build0, E = p.limit.Build()
    if E != nil {
        return "", elist.Push(E, "Psql_select::Build: p.limit.Build()")
    }
    res += build0

    // End of the query string.
    res += ";"

    return res, nil
}   // End of function Psql_select::Build.
