# duckegg

Duckegg is an embedded python datalog implementation built around duckdb to supply it's core functionality. Duckdb is making waves as a performant, embedded, easy-to-deploy OLAP database

Blog post here <https://www.philipzucker.com/duckegg-post/>

Really duckegg is a designed to be a relational egglog. Egraphs are a data structure for performing nodestructive term rewriting and equational reasoning. This graph can be represented and queried as tables (each enode becomes a row in the corresponding table of it's symbol). The various rebuilding operation can also be represented as SQL operations. The hope basically is that duckdb is so good that the translation cost into SQL is worth it.

Yihong Zhang previously had the idea of building a [relational egglog around sqlite in racket](https://github.com/yihozhang/egraph-sqlite) [PLDI workshop paper](https://src.acm.org/binaries/content/assets/src/2022/yihong-zhang.pdf), and this implementation is very much related and inspired by that one.

For more on egraphs and egglog

- <https://egraphs-good.github.io/>
- <https://www.philipzucker.com/egglog/>
- <https://www.philipzucker.com/souffle-egg4/>
- <https://www.hytradboi.com/2022/writing-part-of-a-compiler-in-datalog>
- <https://www.philipzucker.com/notes/Logic/egraphs/>



