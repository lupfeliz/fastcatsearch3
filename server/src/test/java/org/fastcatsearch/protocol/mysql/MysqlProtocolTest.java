package org.fastcatsearch.protocol.mysql;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.fastcatsearch.ir.io.DataInput;
import org.fastcatsearch.ir.io.DataOutput;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.JsonExpression;
import net.sf.jsqlparser.expression.KeepExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.MySQLGroupConcat;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.NumericBind;
import net.sf.jsqlparser.expression.OracleHierarchicalExpression;
import net.sf.jsqlparser.expression.OracleHint;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.UserVariable;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.WithinGroupExpression;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.expression.operators.relational.RegExpMySQLOperator;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.SetStatement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByVisitor;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.TableFunction;
import net.sf.jsqlparser.statement.select.ValuesList;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

public class MysqlProtocolTest {
	private static final Logger logger = LoggerFactory.getLogger(MysqlProtocolTest.class);
	
	@Test public void testFakeServer() throws Exception {
		/**
		 * NOTE: test requires mysql or mariadb jdbc jar,  run with classpath
		 * define PORT_NUMBER like "-DPORT_NUMBER=3306"
		 **/
		String PORT_NUMBER = System.getProperty("PORT_NUMBER");
		String DO_TEST = System.getProperty("DO_TEST");
		if ( "Y".equals(DO_TEST) &&
			!(PORT_NUMBER == null || "".equals(PORT_NUMBER))) {
			int portNumber = Integer.parseInt(PORT_NUMBER);
			final ServerSocket[] ssock = new ServerSocket[1];
			try {
				ssock[0] = new ServerSocket(portNumber);
				final Thread mt = new Thread() {
					@Override public void run() {
						while (true) {
							try {
								Runnable r = new PacketProcessThread(ssock[0].accept());
								Thread st = new Thread(r);
								st.start();
							} catch (Exception e) {
							}
						}
					}
				};
				mt.start();
				try {
					Class.forName("org.mariadb.jdbc.Driver");
					Connection con = DriverManager.getConnection("jdbc:mysql://localhost:" + portNumber + "/db", "user", "password");
					final Statement stm = con.createStatement();
					Thread[]threads = new Thread[10];
					for (int inx = 0; inx < threads.length; inx++) {
						threads[inx] = new Thread() {
							@Override public void run() {
								try {
									Random random = new Random();
									int num = Math.abs(random.nextInt()) % 100;
									ResultSet res1 = stm.executeQuery("SELECT C1, C" + num + " FROM TABLE LIMIT 1");
									ResultSetMetaData meta = res1.getMetaData();
									while (res1.next()) {
										StringBuilder sb = new StringBuilder();
										for (int cinx = 1; cinx <= meta.getColumnCount(); cinx++) {
											if (cinx == 1) { sb.append("|"); }
											sb.append(res1.getString(cinx))
												.append("|");
										}
										logger.debug("{}", sb);
									}
								} catch (Exception e) { }
							}
						};
					}
					for (int inx = 0; inx < threads.length; inx++) {
						threads[inx].start();
					}
				} catch (Exception e) {
					logger.error("", e);
				}
				mt.join();
			} catch (Exception e) { 
				logger.error("", e);
			} finally {
				if (ssock[0] != null) try { ssock[0].close(); } catch (Exception ignore) { }
			}
			assertTrue(true);
		}
	}

	static class PacketProcessThread implements Runnable {
		private Socket sock;
		public PacketProcessThread(Socket sock) {
			this.sock = sock;
		}
		@Override public void run() {
			int COL_CNT = 2;
			Random random = new Random();
			AtomicInteger phase = new AtomicInteger();
			PacketDataInput input = null;
			PacketDataOutput output = null;
			try {
				byte[] buf = new byte[9999];
				input = new PacketDataInput(sock.getInputStream(), buf);
				output = new PacketDataOutput(sock.getOutputStream(), buf);
				int phaseNum = 0, phasePrev = 0;
				byte[][] authData = new byte[][] {
					{0, 0, 0, 0, 0, 0, 0, 0},
					{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
				};
				while (true) {
					phaseNum = phase.get();
					switch (phaseNum) {
					case 0: 
						HandshakePacket packet1 = new HandshakePacket();
						for (int inx1 = 0; inx1 < authData.length; inx1++) {
							for (int inx2 = 0; inx2 < authData[inx1].length; inx2++) {
								authData[inx1][inx2] = (byte)(random.nextInt() & 0xff);
							}
						}
						packet1.setConnectionId(52);
						packet1.setCharset((byte)21);
						packet1.setProtocolVersion(10);
						packet1.setServerVersion("5.5.5-10.1.26-MariaDB");
						packet1.setAuthData(authData);
						packet1.setCapabilities(0xa03ff7ff);
						packet1.setStatus(0x200);
						packet1.setAuthMethod("mysql_native_password");
						packet1.setAuthDataLen((byte)21);
						packet1.writeTo(output);
						logger.debug("HANDSHAKE\n{}", packet1);
						phase.incrementAndGet();
						break;
					case 1:
						AuthPacket packet2 = new AuthPacket();
						packet2.readFrom(input);
						logger.debug("AUTH\n{}", packet2);
						logger.debug("password 'password' is (for login) {}", 
							Packet.binHex(AuthPacket.password("password", authData[0], authData[1]), -1, -1, true));
						logger.debug("password 'password' is (for store) {}", 
							Packet.binHex(AuthPacket.password("password", null, null), -1, -1, true));
						
						byte[] packet3 = new byte[] { 0x07, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00 };
						output.writeBytes(packet3, 0, packet3.length);

						phase.incrementAndGet();
						break;
					default:
						CommandPacket packet4 = new CommandPacket();
						packet4.readFrom(input);
						logger.debug("DEBUG\n{}", packet4);
						
						ResultSetPacket packet5 = new ResultSetPacket();
						List<ColumnPacket> columns = new ArrayList<ColumnPacket>();
						ColumnPacket column = null;
						if (packet4.getArgs().startsWith("show variables like 'max_allowed_packet'")) {
							column = new ColumnPacket(); column.setCatalog("def"); column.setName("Variable_name"); columns.add(column);
							column = new ColumnPacket(); column.setCatalog("def"); column.setName("Value"); columns.add(column);
						} else {
							for (int inx = 0; inx < COL_CNT; inx++) {
								column = new ColumnPacket(); 
								column.setCatalog("def"); 
								column.setName("COL-" + inx); 
								column.setCharset(0x0c);
								columns.add(column);
							}
						}
						packet5.setColumns(columns);
						final List<List<String>> rowData = new ArrayList<List<String>>();
						if (packet4.getArgs().startsWith("show variables like 'max_allowed_packet'")) {
							String[] colData = new String[columns.size()];
							colData[0] = "max_allowed_packet";
							colData[1] = "16777216";
							rowData.add(Arrays.asList(colData));
						} else {
							for (int rinx = 0; rinx < 100; rinx++) {
								List<String> colData = new ArrayList<String>();
								for (int cinx = 0; cinx < columns.size(); cinx++) { 
									int num = Math.abs(random.nextInt()) % 100;
									colData.add("[" + num + "]" + packet4.getArgs());
								}
								rowData.add(colData);
							}
						}
						packet5.setData(rowData.iterator());
						packet5.writeTo(output);
						
						phase.incrementAndGet();
						break;
					}
					if (phaseNum == phasePrev) { try { Thread.sleep(500); } catch (Exception ignore) { } }
					phasePrev = phaseNum;
				}
			} catch (Exception e) {
				logger.error("", e);
			} finally {
				if (input != null) try { input.close(); } catch (Exception ignore) { }
				if (output != null) try { output.close(); } catch (Exception ignore) { }
			}
		}
	}
		
	
	@Test
	public void testPassthrough() throws Exception {
		String DO_TEST = System.getProperty("DO_TEST");
		final String PORT_NUMBER1 = System.getProperty("PORT_NUMBER1"); //ex : 3306
		final String PORT_NUMBER2 = System.getProperty("PORT_NUMBER2"); //ex : 3307
		final String USER_NAME = System.getProperty("USER_NAME");
		final String PASSWORD = System.getProperty("PASSWORD");
		final String QUERY_STR = System.getProperty("QUERY_STR"); //ex : SELECT * FROM TEST.TEST
		if ("Y".equals(DO_TEST) &&
				PORT_NUMBER1 != null && PORT_NUMBER2 != null && !"".equals(PORT_NUMBER1) && !"".equals(PORT_NUMBER2)) {
			final int portNumber1 = Integer.parseInt(PORT_NUMBER1);
			final int portNumber2 = Integer.parseInt(PORT_NUMBER2);
			
			final byte[][][] authData = new byte[1][][];
			final ServerSocket ssock = new ServerSocket(portNumber2);
			final Socket[] sock = new Socket[2];
			final InputStream[] istream = new InputStream[2];
			final OutputStream[] ostream = new OutputStream[2];
			final AtomicInteger phase = new AtomicInteger();
			final Thread tt = new Thread() {
				public void run() {
					//SERVER THREAD
					try {
						sock[1] = new Socket("localhost", portNumber1);
						istream[1] = sock[1].getInputStream();
						ostream[1] = sock[1].getOutputStream();
						while (true) {
							try { passthroughServer(phase, istream[1], ostream[0], authData); } 
							catch (Exception e) { logger.error("", e); }
						}
					} catch (Exception e) {
						logger.error("", e);
					}
				}
			};
			
			final Thread st = new Thread() {
				public void run() {
					//CLIENT THREAD
					try {
						istream[0] = sock[0].getInputStream();
						ostream[0] = sock[0].getOutputStream();
						while (true) {
							try { passthroughClient(phase, istream[0], ostream[1], authData); } 
							catch (Exception e) { logger.error("", e); }
						}
					} catch (Exception e) {
						logger.error("", e);
					}
				}
			};
			
			Thread mt = new Thread() {
				public void run() {
					try {
						for (; ; ) {
							sock[0] = ssock.accept();
							st.start();
							tt.start();
						}
					} catch (Exception e) { 
						logger.error("", e);
					}
				}
			};
			mt.start();
			try {
				Class.forName("org.mariadb.jdbc.Driver");
				Connection con = DriverManager.getConnection("jdbc:mysql://localhost:" + portNumber2 + "", USER_NAME, PASSWORD);
				Statement stm = con.createStatement();
				ResultSet res = stm.executeQuery(QUERY_STR);
				ResultSetMetaData meta = res.getMetaData();
				while (res.next()) {
					StringBuilder sb = new StringBuilder();
					for (int cinx = 1; cinx <= meta.getColumnCount(); cinx++) {
						if (cinx == 1) { sb.append("|"); }
						sb.append(res.getString(cinx)).append("|");
					}
					logger.debug("{}", sb);
				}
			} catch (Exception e) {
				logger.error("", e);
			}
			mt.join();
			ssock.close();
		}
		assertTrue(true);
	}
	
	public void passthroughServer(AtomicInteger phase, InputStream istream, OutputStream ostream, byte[][][] authData) throws IOException {
		byte[] buffer = new byte[9999];
		Packet packet = null;
		PacketDataInput input = new PacketDataInput(istream, buffer);
		PacketDataOutput output = new PacketDataOutput(ostream, buffer);
		
		
		int phaseNum = phase.get();
		if (phaseNum == 0) {
			packet = new HandshakePacket();
			packet.readFrom(input);
			HandshakePacket p = (HandshakePacket) packet;
			logger.debug("HANDSHAKE\n{}", p);
			authData[0] = p.getAuthData();
			p.writeTo(output);
			
			phase.getAndIncrement();
		} else if (phaseNum == 2) {
			int rl = istream.read(buffer, 0, buffer.length);
			logger.debug("OKPACKET\n{}", Packet.binHex(buffer, 0, rl, true));
			ostream.write(buffer, 0, rl);
			//packet = new OKPacket();
			//packet.read(ByteBuffer.wrap(buf));
			phase.getAndIncrement();
		} else if (phaseNum == 4) {
			packet = new ResultSetPacket();
			packet.readFrom(input);
			ResultSetPacket p = (ResultSetPacket) packet;
			p.writeTo(output);
			phase.getAndIncrement();
		} else if (phaseNum > 4) {
			try {
				if (phaseNum % 2 == 0) {
					packet = new ResultSetPacket();
					packet.readFrom(input);
					ResultSetPacket p = (ResultSetPacket) packet;
					p.writeTo(output);
					phase.getAndIncrement();
				}
			} catch (Exception e) {
				logger.error("", e);
			}
		}
	}
	
	public void passthroughClient (AtomicInteger phase, InputStream istream, OutputStream ostream, byte[][][] authData) throws IOException {
		Packet packet = null;
		byte[] buffer = new byte[9999];
		DataInput input = new PacketDataInput(istream, buffer);
		DataOutput output = new PacketDataOutput(ostream, buffer);
		
		int phaseNum = phase.get();
		if (phaseNum == 1) {
			////////////////////////////////////////////////////////////////////////////////
			int rl = istream.read(buffer, 0, buffer.length);
			ostream.write(buffer, 0, rl);
			phase.getAndIncrement();
			////////////////////////////////////////////////////////////////////////////////
		} else if (phaseNum == 3) {
			packet = new CommandPacket();
			packet.readFrom(input);
			CommandPacket p = (CommandPacket) packet;
			logger.debug("COLUMNS \n{}", p);
			p.writeTo(output);
			
			phase.getAndIncrement();
		} else if (phaseNum > 4) {
			try {
				if (phaseNum % 2 == 1) {
					packet = new CommandPacket();
					packet.readFrom(input);
					CommandPacket p = (CommandPacket) packet;
					p.writeTo(output);
					logger.debug("COMMAND\n{}", p);
					phase.getAndIncrement();
				}
			} catch (Exception e) {
				logger.error("", e);
			}
		}
	}
	
	@Test public void testSQLParser() throws Exception {
		String DO_TEST = System.getProperty("DO_TEST");
		if ("Y".equals(DO_TEST)) {
			String[] quries = new String[] {
				"SELECT * FROM TEST.TEST WHERE MATCH('') ORDER BY TEST LIMIT 10, 10",
				"SELECT * FROM TEST WHERE MATCH('{NAME:테스트:10:16}')"
			};
			CCJSqlParser  parser;
			SQLProcessor processor = new SQLProcessor();
			for (String query : quries) {
				parser = new CCJSqlParser(new StringReader(query));
				parser.Statement().accept(processor);
			}
		}
	}
	
	static class SQLProcessor implements StatementVisitor, SelectVisitor, ExpressionVisitor, FromItemVisitor,
		ItemsListVisitor, OrderByVisitor, SelectItemVisitor {

		@Override public void visit(ExpressionList expressionList) {
			logger.debug("{}", expressionList);
		}

		@Override public void visit(MultiExpressionList multiExprList) {
			logger.debug("{}", multiExprList);
		}

		@Override public void visit(Table tableName) {
			logger.debug("TABLENAME : {}", tableName);
		}

		@Override public void visit(SubSelect subSelect) {
			logger.debug("SUBSELECT : {}", subSelect);
		}

		@Override public void visit(SubJoin subjoin) {
			logger.debug("{}", subjoin);
		}

		@Override public void visit(LateralSubSelect lateralSubSelect) {
			logger.debug("{}", lateralSubSelect);
		}

		@Override public void visit(ValuesList valuesList) {
			logger.debug("{}", valuesList);
		}

		@Override public void visit(TableFunction tableFunction) {
			logger.debug("{}", tableFunction);
		}

		@SuppressWarnings("unchecked") @Override public void visit(PlainSelect plainSelect) {
			logger.debug("PLAINSELECT : {}", plainSelect);
			Object stm = null;
			if ((stm = plainSelect.getWhere()) != null) { 
				((Expression)stm).accept(this); 
			}
			if ((stm = plainSelect.getOrderByElements()) != null) { 
				for (OrderByElement item : ((List<OrderByElement>) stm)) {
					item.accept(this);
				}
			}
			if ((stm = plainSelect.getGroupByColumnReferences()) != null) { 
				for (Expression item : ((List<Expression>) stm)) {
					item.accept(this);
				}
			}
			if ((stm = plainSelect.getLimit()) != null) { 
				Limit limit = (Limit) stm;
				logger.debug("LIMIT:{}", limit);
			}
			if ((stm = plainSelect.getFromItem()) != null) { 
				((FromItem)stm).accept(this);
			}
			if ((stm = plainSelect.getSelectItems()) != null) {
				for (SelectItem item : ((List<SelectItem>) stm)) {
					item.accept(this);
				}
			}
		}

		@Override public void visit(SetOperationList setOpList) {
			logger.debug("{}", setOpList);
		}

		@Override public void visit(WithItem withItem) {
			logger.debug("{}", withItem);
		}

		@Override public void visit(NullValue nullValue) {
			logger.debug("{}", nullValue);
		}

		@Override public void visit(Function function) {
			logger.debug("FUNCTION : {}", function);
		}

		@Override public void visit(SignedExpression signedExpression) {
			logger.debug("{}", signedExpression);
		}

		@Override public void visit(JdbcParameter jdbcParameter) {
			logger.debug("{}", jdbcParameter);
		}

		@Override public void visit(JdbcNamedParameter jdbcNamedParameter) {
			logger.debug("{}", jdbcNamedParameter);
		}

		@Override public void visit(DoubleValue doubleValue) {
			logger.debug("{}", doubleValue);
		}

		@Override public void visit(LongValue longValue) {
			logger.debug("{}", longValue);
		}

		@Override public void visit(HexValue hexValue) {
			logger.debug("{}", hexValue);
		}

		@Override public void visit(DateValue dateValue) {
			logger.debug("{}", dateValue);
		}

		@Override public void visit(TimeValue timeValue) {
			logger.debug("{}", timeValue);
		}

		@Override public void visit(TimestampValue timestampValue) {
			logger.debug("{}", timestampValue);
		}

		@Override public void visit(Parenthesis parenthesis) {
			logger.debug("{}", parenthesis);
		}

		@Override public void visit(StringValue stringValue) {
			logger.debug("{}", stringValue);
		}

		@Override public void visit(Addition addition) {
			logger.debug("{}", addition);
		}

		@Override public void visit(Division division) {
			logger.debug("{}", division);
		}

		@Override public void visit(Multiplication multiplication) {
			logger.debug("{}", multiplication);
		}

		@Override public void visit(Subtraction subtraction) {
			logger.debug("{}", subtraction);
		}

		@Override public void visit(AndExpression andExpression) {
			logger.debug("{}", andExpression);
		}

		@Override public void visit(OrExpression orExpression) {
			logger.debug("{}", orExpression);
		}

		@Override public void visit(Between between) {
			logger.debug("{}", between);
		}

		@Override public void visit(EqualsTo equalsTo) {
			logger.debug("{}", equalsTo);
		}

		@Override public void visit(GreaterThan greaterThan) {
			logger.debug("{}", greaterThan);
		}

		@Override public void visit(GreaterThanEquals greaterThanEquals) {
			logger.debug("{}", greaterThanEquals);
		}

		@Override public void visit(InExpression inExpression) {
			logger.debug("{}", inExpression);
		}

		@Override public void visit(IsNullExpression isNullExpression) {
			logger.debug("{}", isNullExpression);
		}

		@Override public void visit(LikeExpression likeExpression) {
			logger.debug("{}", likeExpression);
		}

		@Override public void visit(MinorThan minorThan) {
			logger.debug("{}", minorThan);
		}

		@Override public void visit(MinorThanEquals minorThanEquals) {
			logger.debug("{}", minorThanEquals);
		}

		@Override public void visit(NotEqualsTo notEqualsTo) {
			logger.debug("{}", notEqualsTo);
		}

		@Override public void visit(Column tableColumn) {
			logger.debug("{}", tableColumn);
		}

		@Override public void visit(CaseExpression caseExpression) {
			logger.debug("{}", caseExpression);
		}

		@Override public void visit(WhenClause whenClause) {
			logger.debug("{}", whenClause);
		}

		@Override public void visit(ExistsExpression existsExpression) {
			logger.debug("{}", existsExpression);
		}

		@Override public void visit(AllComparisonExpression allComparisonExpression) {
			logger.debug("{}", allComparisonExpression);
		}

		@Override public void visit(AnyComparisonExpression anyComparisonExpression) {
			logger.debug("{}", anyComparisonExpression);
		}

		@Override public void visit(Concat concat) {
			logger.debug("{}", concat);
		}

		@Override public void visit(Matches matches) {
			logger.debug("{}", matches);
		}

		@Override public void visit(BitwiseAnd bitwiseAnd) {
			logger.debug("{}", bitwiseAnd);
		}

		@Override public void visit(BitwiseOr bitwiseOr) {
			logger.debug("{}", bitwiseOr);
		}

		@Override public void visit(BitwiseXor bitwiseXor) {
			logger.debug("{}", bitwiseXor);
		}

		@Override public void visit(CastExpression cast) {
			logger.debug("{}", cast);
		}

		@Override public void visit(Modulo modulo) {
			logger.debug("{}", modulo);
		}

		@Override public void visit(AnalyticExpression aexpr) {
			logger.debug("{}", aexpr);
		}

		@Override public void visit(WithinGroupExpression wgexpr) {
			logger.debug("{}", wgexpr);
		}

		@Override public void visit(ExtractExpression eexpr) {
			logger.debug("{}", eexpr);
		}

		@Override public void visit(IntervalExpression iexpr) {
			logger.debug("{}", iexpr);
		}

		@Override public void visit(OracleHierarchicalExpression oexpr) {
			logger.debug("{}", oexpr);
		}

		@Override public void visit(RegExpMatchOperator rexpr) {
			logger.debug("{}", rexpr);
		}

		@Override public void visit(JsonExpression jsonExpr) {
			logger.debug("{}", jsonExpr);
		}

		@Override public void visit(RegExpMySQLOperator regExpMySQLOperator) {
			logger.debug("{}", regExpMySQLOperator);
		}

		@Override public void visit(UserVariable var) {
			logger.debug("{}", var);
		}

		@Override public void visit(NumericBind bind) {
			logger.debug("{}", bind);
		}

		@Override public void visit(KeepExpression aexpr) {
			logger.debug("{}", aexpr);
		}

		@Override public void visit(MySQLGroupConcat groupConcat) {
			logger.debug("{}", groupConcat);
		}

		@Override public void visit(RowConstructor rowConstructor) {
			logger.debug("{}", rowConstructor);
		}

		@Override public void visit(OracleHint hint) {
			logger.debug("{}", hint);
		}

		@Override public void visit(Select select) {
			logger.debug("VISIT SELECT : {}", select);
			if (select.getWithItemsList() != null) {
				for (WithItem withItem : select.getWithItemsList()) {
					withItem.accept(this);
				}
			}
			select.getSelectBody().accept(this);
		}

		@Override public void visit(Delete delete) {
			logger.debug("{}", delete);
		}

		@Override public void visit(Update update) {
			logger.debug("{}", update);
		}

		@Override public void visit(Insert insert) {
			logger.debug("{}", insert);
		}

		@Override public void visit(Replace replace) {
			logger.debug("{}", replace);
		}

		@Override public void visit(Drop drop) {
			logger.debug("{}", drop);
		}

		@Override public void visit(Truncate truncate) {
			logger.debug("{}", truncate);
		}

		@Override public void visit(CreateIndex createIndex) {
			logger.debug("{}", createIndex);
		}

		@Override public void visit(CreateTable createTable) {
			logger.debug("{}", createTable);
		}

		@Override public void visit(CreateView createView) {
			logger.debug("{}", createView);
		}

		@Override public void visit(Alter alter) {
			logger.debug("{}", alter);
		}

		@Override public void visit(Statements stmts) {
			logger.debug("{}", stmts);
		}

		@Override public void visit(Execute execute) {
			logger.debug("{}", execute);
		}

		@Override public void visit(SetStatement set) {
			logger.debug("{}", set);
		}

		@Override public void visit(Merge merge) {
			logger.debug("{}", merge);
		}

		@Override public void visit(OrderByElement orderBy) {
			logger.debug("ORDERBY : {}", orderBy);
		}

		@Override public void visit(AllColumns allColumns) {
			logger.debug("ALLCOLUMNS : {}", allColumns);
		}

		@Override public void visit(AllTableColumns allTableColumns) {
			logger.debug("ALLTABLECOLUMNS : {}", allTableColumns);
		}

		@Override public void visit(SelectExpressionItem selectExpressionItem) {
			logger.debug("SELECTEXP : {}", selectExpressionItem);
		}
	}
}