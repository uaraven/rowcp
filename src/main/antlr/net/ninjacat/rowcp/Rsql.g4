grammar Rsql;

@header {
package net.ninjacat.rowcp;
}

query: (selectStatement ';'?)+ EOF;

where: K_WHERE anything;

anything: .*?;

distinct: K_DISTINCT;

projection: distinct? K_ALL;

selectStatement: K_SELECT projection K_FROM sourceName where?;

alias: IDENTIFIER;

sourceName: name alias?;

name: IDENTIFIER;

K_ALL: '*';
K_AS: A S;
K_FROM: F R O M;
K_SELECT: S E L E C T;
K_DISTINCT: D I S T I N C T;
K_WHERE: W H E R E;

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