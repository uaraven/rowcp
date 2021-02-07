grammar Rsql;

@header {
package net.ninjacat.dtc;
}

query: selectStatement ';'? EOF;

operator: '<' | '<=' | '>' | '>=' | '=' | '!=' | '<>';

list: '(' literalValue (',' literalValue)* ')';

term: compoundName | literalValue;

expr:
	term operator term							# condition
	| expr K_AND expr							# andExpr
	| expr K_OR expr							# orExpr
	| expr K_IS K_NOT? K_NULL                                               # isNullExpr
	| K_NOT expr								# notExpr
	| '(' expr ')'								# parensExpr
	| term K_BETWEEN expr K_AND expr			# betweenExpr
	| term K_NOT? K_LIKE stringValue			# likeExpr
	| term K_NOT? K_IN list						# inExpr
	| term K_NOT? K_IN '(' selectStatement ')'	# subSelectExpr;

where: K_WHERE expr;

distinct: K_DISTINCT;

projection: distinct? K_ALL;

selectStatement: K_SELECT projection K_FROM sourceName where?;

signedNumber: ( '+' | '-')? NUMERIC_LITERAL;

stringValue: STRING_LITERAL;

nullValue: K_NULL;

literalValue: signedNumber | stringValue | nullValue;

alias: IDENTIFIER;

sourceName: name alias?;

compoundName: (qualifier '.')? name;

name: IDENTIFIER;

qualifier: IDENTIFIER;

K_ALL: '*';
K_AND: A N D;
K_AS: A S;
K_BETWEEN: B E T W E E N;
K_FROM: F R O M;
K_IN: I N;
K_NOT: N O T;
K_NULL: N U L L;
K_OR: O R;
K_REGEX: R E G E X;
K_SELECT: S E L E C T;
K_MATCH: M A T C H;
K_WHERE: W H E R E;
K_TRUE: T R U E;
K_FALSE: F A L S E;
K_JOIN: J O I N;
K_LEFT: L E F T;
K_RIGHT: R I G H T;
K_OUTER: O U T E R;
K_INNER: I N N E R;
K_CROSS: C R O S S;
K_ON: O N;
K_DISTINCT: D I S T I N C T;
K_LIKE: L I K E;
K_IS: I S;

IDENTIFIER: SIMPLE_IDENTIFIER | QUOTED_IDENTIFIER;

SIMPLE_IDENTIFIER: [a-zA-Z] [a-zA-Z_0-9]*;

QUOTED_IDENTIFIER: '"' ( ~'"' | '""')* '"';

NUMERIC_LITERAL:
	DIGIT+ ('.' DIGIT*)? (E [-+]? DIGIT+)?
	| '.' DIGIT+ ( E [-+]? DIGIT+)?;

STRING_LITERAL: '\'' (~'\'' | '\'\'')* '\'';

SINGLE_LINE_COMMENT: '--' ~[\r\n]* -> channel(HIDDEN);

MULTILINE_COMMENT: '/*' .*? ( '*/' | EOF) -> channel(HIDDEN);

SPACES: [ \u000B\t\r\n] -> channel(HIDDEN);

UNEXPECTED_CHAR: .;

fragment DIGIT: [0-9];

fragment A: [aA];
fragment B: [bB];
fragment C: [cC];
fragment D: [dD];
fragment E: [eE];
fragment F: [fF];
fragment G: [gG];
fragment H: [hH];
fragment I: [iI];
fragment J: [jJ];
fragment K: [kK];
fragment L: [lL];
fragment M: [mM];
fragment N: [nN];
fragment O: [oO];
fragment P: [pP];
fragment Q: [qQ];
fragment R: [rR];
fragment S: [sS];
fragment T: [tT];
fragment U: [uU];
fragment V: [vV];
fragment W: [wW];
fragment X: [xX];
fragment Y: [yY];
fragment Z: [zZ];