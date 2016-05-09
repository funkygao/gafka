// mysql protocol basics.
package server

func (cc *clientConn) dispatch(data []byte) error {
	cmd := data[0]
	data = data[1:]
	cc.lastCmd = hack.String(data)

	token := cc.server.getToken()

	defer func() {
		cc.server.releaseToken(token)
	}()

	switch cmd {
	case mysql.ComQuit:
		return io.EOF

	case mysql.ComQuery:
		return cc.handleQuery(hack.String(data))

	case mysql.ComPing:
		return cc.writeOK()

	case mysql.ComInitDB:
		log.Debug("init db", hack.String(data))
		if err := cc.useDB(hack.String(data)); err != nil {
			return errors.Trace(err)
		}
		return cc.writeOK()

	case mysql.ComFieldList:
		return cc.handleFieldList(hack.String(data))

	case mysql.ComStmtPrepare:
		return cc.handleStmtPrepare(hack.String(data))

	case mysql.ComStmtExecute:
		return cc.handleStmtExecute(data)

	case mysql.ComStmtClose:
		return cc.handleStmtClose(data)

	case mysql.ComStmtSendLongData:
		return cc.handleStmtSendLongData(data)

	case mysql.ComStmtReset:
		return cc.handleStmtReset(data)

	}
}
