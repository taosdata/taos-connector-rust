use taos_ws::Stmt2 as WsStmt2;

pub struct Stmt2(Stmt2Inner);

enum Stmt2Inner {
    Ws(WsStmt2),
}
