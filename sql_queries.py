from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Query import Query
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;"
                     "CREATE TABLE Query("
                     "q_id INTEGER NOT NULL PRIMARY KEY CHECK (q_id > 0),"
                     "q_purpose TEXT NOT NULL,"
                     "q_size INTEGER NOT NULL CHECK (q_size >= 0),"
                     "UNIQUE (q_id));"
                     "CREATE TABLE Disk("
                     "d_id INTEGER NOT NULL PRIMARY KEY UNIQUE CHECK (d_id > 0),"
                     "d_company TEXT NOT NULL,"
                     "d_speed INTEGER NOT NULL CHECK (d_speed > 0),"
                     "d_space INTEGER NOT NULL CHECK (d_space >= 0),"
                     "d_cost INTEGER NOT NULL CHECK (d_cost > 0));"
                     "CREATE TABLE Ram("
                     "r_id INTEGER NOT NULL PRIMARY KEY UNIQUE CHECK (r_id > 0),"
                     "r_company TEXT NOT NULL,"
                     "r_size INTEGER NOT NULL CHECK (r_size > 0));"
                     "CREATE TABLE DiskQuery("
                     "d_id INTEGER REFERENCES Disk ON DELETE CASCADE ,"
                     "q_id INTEGER REFERENCES Query ON DELETE CASCADE, "
                     "PRIMARY KEY(d_id, q_id)); "
                     "CREATE TABLE DiskRam("
                     "d_id INTEGER REFERENCES Disk ON DELETE CASCADE, "
                     "r_id INTEGER REFERENCES Ram ON DELETE CASCADE, "
                     "PRIMARY KEY(d_id, r_id)); "
                     "CREATE VIEW CanRunOnDisk as "
                     "SELECT D.d_id, Q.q_id "
                     "FROM Disk D, Query Q "
                     "WHERE Q.q_size <= D.d_space;"
                     "CREATE VIEW CanRunCount as "
                     "SELECT D.d_id, D.d_speed, (SELECT COUNT(*) "
                     "FROM CanRunOnDisk C WHERE C.d_id =D.d_id) as Count "
                     "FROM Disk D; "
                     "COMMIT;"
                     )

    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except DatabaseException.database_ini_ERROR as e:
        print(e)
    except DatabaseException.UNKNOWN_ERROR as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()


def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;"
                     "DELETE FROM Query CASCADE;"
                     "DELETE FROM  Disk CASCADE;"
                     "DELETE FROM Ram CASCADE;"
                     "DELETE FROM DiskQuery CASCADE;"
                     "DELETE FROM DiskRam CASCADE;"
                     "COMMIT;"
                     )

    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        print("npo")
    except DatabaseException.database_ini_ERROR as e:
        print(e)
    except DatabaseException.UNKNOWN_ERROR as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;"
                     "DROP TABLE IF EXISTS Query CASCADE;"
                     "DROP TABLE IF EXISTS Disk CASCADE;"
                     "DROP TABLE IF EXISTS Ram CASCADE;"
                     "DROP TABLE IF EXISTS DiskQuery CASCADE;"
                     "DROP TABLE IF EXISTS DiskRam CASCADE;"
                     "DROP VIEW IF EXISTS CanRunOnDisk;"
                     "DROP VIEW IF EXISTS CanRunCount;"
                     "COMMIT;"
                     )

    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except DatabaseException.database_ini_ERROR as e:
        print(e)
    except DatabaseException.UNKNOWN_ERROR as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()


def addQuery(query: Query) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Query(q_id, q_purpose, q_size) VALUES({q_id}, {q_purpose}, {q_size});").format(
            q_id=sql.Literal(query.getQueryID()), q_purpose=sql.Literal(query.getPurpose()),
            q_size=sql.Literal(query.getSize()))
        conn.execute(query)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION:
        conn.close()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        conn.close()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        conn.close()
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.database_ini_ERROR:
        conn.close()
        return ReturnValue.ERROR
    except DatabaseException.UNKNOWN_ERROR as e:
        conn.close()
        return ReturnValue.ERROR
    except DatabaseException.ConnectionInvalid:
        conn.close()
        return ReturnValue.ERROR
    except Exception:
        conn.close()
        return ReturnValue.ERROR

    conn.close()
    return ReturnValue.OK


def getQueryProfile(queryID: int) -> Query:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT q_purpose, q_size"
                        " FROM Query"
                        " WHERE q_id = {id}").format(id=sql.Literal(queryID))
        rows_effected, res = conn.execute(query)
        conn.commit()
    except Exception:
        conn.close()
        return Query.badQuery()

    conn.close()
    if rows_effected <= 0:
        return Query.badQuery()
    else:
        return Query(queryID, res[0]['q_purpose'], res[0]['q_size'])


def deleteQuery(query: Query) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        "UPDATE Disk SET d_space = d_space +"
                        "   (SELECT q_size FROM Query WHERE q_id = {q_id})"
                        "WHERE d_id in "
                        "   (SELECT d_id FROM DiskQuery WHERE q_id = {q_id});"
                        "DELETE FROM DiskQuery WHERE q_id = {q_id};"
                        "DELETE FROM Query WHERE q_id = {q_id};"
                        "COMMIT; ").format(q_id=sql.Literal(query.getQueryID()))
        conn.execute(query)
    except Exception as e:
        conn.rollback()
        conn.close()
        return ReturnValue.ERROR

    conn.close()
    return ReturnValue.OK


def addDisk(disk: Disk) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Disk VALUES({d_id}, {d_company}, {d_speed}, {d_space}, {d_cost});").format(
            d_id=sql.Literal(disk.getDiskID()), d_company=sql.Literal(disk.getCompany()),
            d_speed=sql.Literal(disk.getSpeed()), d_space=sql.Literal(disk.getFreeSpace()),
            d_cost=sql.Literal(disk.getCost()))
        conn.execute(query)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION:
        conn.close()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        conn.close()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        conn.close()
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.database_ini_ERROR:
        conn.close()
        return ReturnValue.ERROR
    except DatabaseException.UNKNOWN_ERROR as e:
        conn.close()
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.ConnectionInvalid:
        conn.close()
        return ReturnValue.ERROR
    except Exception:
        conn.close()
        return ReturnValue.ERROR

    conn.close()
    return ReturnValue.OK


def getDiskProfile(diskID: int) -> Disk:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT *"
                        " FROM Disk"
                        " WHERE d_id = {id}").format(id=sql.Literal(diskID))
        rows_effected, res = conn.execute(query)
        conn.commit()
    except Exception:
        conn.close()
        return Disk.badDisk()

    conn.close()
    if rows_effected <= 0:
        return Disk.badDisk()
    else:
        return Disk(res[0]['d_id'], res[0]['d_company'], res[0]['d_speed'], res[0]['d_space'], res[0]['d_cost'])


def deleteDisk(diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        "DELETE FROM DiskQuery WHERE d_id = {d_id};"
                        "DELETE FROM DiskRam WHERE d_id = {d_id};"
                        "DELETE FROM Disk WHERE d_id = {d_id};").format(d_id=sql.Literal(diskID))
        rows_effected, _ = conn.execute(query)
        conn.commit()
        if rows_effected == 0:
            conn.close()
            return ReturnValue.NOT_EXISTS
    except Exception:
        conn.rollback()
        conn.close()
        return ReturnValue.ERROR

    conn.close()
    return ReturnValue.OK


def addRAM(ram: RAM) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Ram VALUES({r_id},{r_company},{r_size});").format(
            r_id=sql.Literal(ram.getRamID()), r_company=sql.Literal(ram.getCompany()),
            r_size=sql.Literal(ram.getSize()))
        conn.execute(query)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION:
        conn.close()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        conn.close()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        conn.close()
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.database_ini_ERROR:
        conn.close()
        return ReturnValue.ERROR
    except DatabaseException.UNKNOWN_ERROR as e:
        conn.close()
        return ReturnValue.ERROR
    except DatabaseException.ConnectionInvalid:
        conn.close()
        return ReturnValue.ERROR
    except Exception:
        conn.close()
        return ReturnValue.ERROR

    conn.close()
    return ReturnValue.OK


def getRAMProfile(ramID: int) -> RAM:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT *"
                        " FROM Ram"
                        " WHERE r_id = {id}").format(id=sql.Literal(ramID))
        rows_effected, res = conn.execute(query)
        conn.commit()
    except Exception:
        conn.close()
        return RAM.badRAM()

    conn.close()
    if rows_effected <= 0:
        return RAM.badRAM()
    else:
        return RAM(res[0]['r_id'], res[0]['r_company'], res[0]['r_size'])


def deleteRAM(ramID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        "DELETE FROM DiskRam WHERE r_id = {r_id};"
                        "DELETE FROM Ram WHERE r_id = {r_id};").format(r_id=sql.Literal(ramID))
        rows_effected, _ = conn.execute(query)
        conn.commit()
        if rows_effected == 0:
            conn.close()
            return ReturnValue.NOT_EXISTS
    except Exception as e:
        conn.rollback()
        conn.close()
        return ReturnValue.ERROR

    conn.close()
    return ReturnValue.OK


def addDiskAndQuery(disk: Disk, query: Query) -> ReturnValue:
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        "INSERT INTO Disk VALUES({d_id}, {d_company}, {d_speed}, {d_space}, {d_cost});"
                        "INSERT INTO Query VALUES({q_id}, {q_purpose}, {q_size});"
                        "COMMIT;").format(
            q_id=sql.Literal(query.getQueryID()),
            q_purpose=sql.Literal(query.getPurpose()),
            q_size=sql.Literal(query.getSize()),
            d_id=sql.Literal(disk.getDiskID()),
            d_company=sql.Literal(disk.getCompany()),
            d_speed=sql.Literal(disk.getSpeed()),
            d_space=sql.Literal(disk.getFreeSpace()),
            d_cost=sql.Literal(disk.getCost()))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.UNIQUE_VIOLATION:
        conn.rollback()
        conn.close()
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.database_ini_ERROR:
        conn.rollback()
        conn.close()
        return ReturnValue.ERROR
    except DatabaseException.UNKNOWN_ERROR:
        conn.rollback()
        conn.close()
        return ReturnValue.ERROR
    except DatabaseException.ConnectionInvalid:
        conn.rollback()
        conn.close()
        return ReturnValue.ERROR
    except Exception:
        conn.rollback()
        conn.close()
        return ReturnValue.ERROR

    conn.close()
    return ReturnValue.OK


def addQueryToDisk(query: Query, diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        "INSERT INTO DiskQuery VALUES({d_id}, {q_id});"
                        "UPDATE Disk SET d_space = d_space - "
                        "(SELECT q_size FROM Query WHERE q_id = {q_id})"
                        " WHERE d_id = {d_id};"

                        "COMMIT;").format(d_id=sql.Literal(diskID),
                                          q_id=sql.Literal(query.getQueryID()))

        rows_effected, _ = conn.execute(query)
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        # will cover the case d_id or q_id not found
        conn.rollback()
        conn.close()
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION:
        conn.rollback()
        conn.close()
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        conn.rollback()
        conn.close()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.NOT_NULL_VIOLATION:
        # case bad query or null diskID
        conn.rollback()
        conn.close()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.ConnectionInvalid:
        conn.rollback()
        conn.close()
        return ReturnValue.ERROR
    except DatabaseException.database_ini_ERROR:
        conn.rollback()
        conn.close()
        return ReturnValue.ERROR
    except Exception:
        conn.rollback()
        conn.close()
        return ReturnValue.ERROR

    conn.close()
    return ReturnValue.OK


def removeQueryFromDisk(query: Query, diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        "UPDATE Disk SET d_space = d_space +"
                        "(SELECT q_size FROM Query WHERE q_id = {q_id})"
                        " WHERE d_id = {d_id} AND EXISTS"
                        "(SELECT FROM DiskQuery WHERE  d_id = {d_id} AND q_id = {q_id}) ;"
                        "DELETE FROM DiskQuery WHERE d_id = {d_id} AND q_id = {q_id};"
                        "COMMIT;").format(d_id=sql.Literal(diskID),
                                          q_id=sql.Literal(query.getQueryID()))

        conn.execute(query)

    except DatabaseException.ConnectionInvalid:
        conn.rollback()
        conn.close()
        return ReturnValue.ERROR
    except DatabaseException.database_ini_ERROR:
        conn.rollback()
        conn.close()
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        conn.rollback()
        conn.close()
        return ReturnValue.OK
    except DatabaseException.CHECK_VIOLATION:
        conn.rollback()
        conn.close()
        return ReturnValue.OK
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        conn.rollback()
        conn.close()
        return ReturnValue.OK
    except Exception as e:
        conn.rollback()
        conn.close()
        return ReturnValue.ERROR

    conn.close()
    return ReturnValue.OK


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO DiskRam VALUES({d_id}, {r_id});").format(d_id=sql.Literal(diskID),
                                                                              r_id=sql.Literal(ramID))

        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        # will cover the case d_id or r_id not found
        conn.close()
        return ReturnValue.NOT_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION:
        # case null id's
        conn.close()
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION:
        conn.close()
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid:
        conn.close()
        return ReturnValue.ERROR
    except DatabaseException.database_ini_ERROR:
        conn.close()
        return ReturnValue.ERROR
    except Exception:
        conn.close()
        return ReturnValue.ERROR
    conn.close()
    return ReturnValue.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM DiskRam WHERE d_id ={d_id} AND r_id = {r_id};").format(d_id=sql.Literal(diskID),
                                                                                            r_id=sql.Literal(ramID))

        rows_effected, _ = conn.execute(query)
        conn.commit()
        if rows_effected == 0:
            conn.close()
            return ReturnValue.NOT_EXISTS

    except DatabaseException.NOT_NULL_VIOLATION:
        conn.close()
        return ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid:
        conn.close()
        return ReturnValue.ERROR
    except DatabaseException.database_ini_ERROR:
        conn.close()
        return ReturnValue.ERROR
    except Exception as e:
        conn.close()
        return ReturnValue.ERROR

    conn.close()
    return ReturnValue.OK


def averageSizeQueriesOnDisk(diskID: int) -> float:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT AVG(Q.q_size) as Avg "
                        "FROM Query Q, DiskQuery D "
                        "WHERE Q.q_id = D.q_id "
                        "GROUP BY D.d_id "
                        "HAVING D.d_id = {d_id};").format(d_id=sql.Literal(diskID))

        rows_effected, res = conn.execute(query)
        conn.commit()
        if rows_effected == 0:
            conn.close()
            return 0
    except DatabaseException.ConnectionInvalid:
        conn.close()
        return -1
    except DatabaseException.database_ini_ERROR:
        conn.close()
        return -1
    except Exception as e:
        conn.close()
        return -1

    conn.close()
    return res[0]['Avg']


def diskTotalRAM(diskID: int) -> int:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT SUM(R.r_size) as sum "
                        "FROM Ram R, DiskRam D "
                        "WHERE R.r_id = D.r_id "
                        "GROUP BY D.d_id "
                        "HAVING D.d_id = {d_id};").format(d_id=sql.Literal(diskID))

        rows_effected, res = conn.execute(query)
        conn.commit()
        if rows_effected == 0:
            conn.close()
            return 0
    except DatabaseException.ConnectionInvalid:
        conn.close()
        return -1
    except DatabaseException.database_ini_ERROR:
        conn.close()
        return -1
    except Exception as e:
        conn.close()
        return -1

    conn.close()
    return res[0]['sum']

def getCostForPurpose(purpose: str) -> int:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT SUM(Q.Q_size*D.D_cost) as total_cost "
                        "FROM Disk D, DiskQuery DQ, Query Q "
                        "WHERE DQ.Q_id = Q.Q_id "
                        "AND DQ.D_id = D.D_id "
                        "AND Q.Q_purpose = {purpose_to_check}; ").format(purpose_to_check=sql.Literal(purpose))

        rows_effected, res = conn.execute(query)
        conn.commit()
        if rows_effected == 0:
            conn.close()
            return 0
    except DatabaseException.ConnectionInvalid:
        conn.close()
        return -1
    except DatabaseException.database_ini_ERROR:
        conn.close()
        return -1
    except Exception as e:
        # print(e)
        conn.close()
        return -1

    conn.close()
    if res[0]['total_cost'] is None:
        return 0
    return res[0]['total_cost']


def getQueriesCanBeAddedToDisk(diskID: int) -> List[int]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(  # "BEGIN;"
            "SELECT Q_id "
            "FROM CanRunOnDisk "
            "WHERE D_id = {diskID} "
            "ORDER BY Q_id DESC "
            "LIMIT 5;").format(diskID=sql.Literal(diskID))

            # "SELECT Q_id "
            # "FROM Query, Disk D "
            # "WHERE D.D_id = {disk_ID} "
            # "AND Q_size <= D.D_space "
            # "ORDER BY Q_id DESC "
            # "LIMIT 5;").format(disk_ID=sql.Literal(diskID))

        rows_effected, res = conn.execute(query)
        conn.commit()

        if rows_effected == 0:
            conn.close()
            return list()
    except Exception as e:
        # print(e)
        conn.close()
        return list()

    conn.close()
    return [res[i]['Q_id'] for i in range(0, rows_effected)]


def getQueriesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(  # "BEGIN;"
            # "SELECT Q_id "
            # "FROM CanRunOnDisk "
            # "WHERE D_id = {diskID} "
            "SELECT Q.Q_id "
            "FROM Query Q, Disk D "
            "WHERE D.D_id = {disk_ID} "
            "AND Q.Q_size <= D.D_space "
            "AND (Q.Q_size=0 OR Q.Q_size <= "
            "(SELECT SUM(R.R_size) "
            "FROM DiskRam DR, Ram R "
            "WHERE DR.D_id = {disk_ID} "
            "AND DR.R_id = R.R_id) ) "
            "ORDER BY Q.Q_id ASC "
            "LIMIT 5;").format(disk_ID=sql.Literal(diskID))

            # "SELECT Q.Q_id "
            # "FROM Disk D, Query Q, DiskRam RD "
            # "WHERE D.D_id = {disk_ID} "
            # "AND Q.Q_size <= D.D_space "
            # "AND Q.Q_size <= "
            # "SUM(SELECT R.R_Size "
            # "FROM Ram R2, DiskRam RD2, QueryDisk QD2 "
            # "WHERE Q.Q_id = QD2.Q_id "
            # "AND R2.R_id = RD.R_id) "
            # "ORDER BY Q.Q_id ASC "
            # "LIMIT 5;").format(disk_ID=sql.Literal(diskID))

        rows_effected, res = conn.execute(query)
        conn.commit()

        if rows_effected == 0:
            conn.close()
            return list()
    except Exception as e:
        # print(e)
        conn.close()
        return list()

    conn.close()
    return [res[i]['Q_id'] for i in range(0, rows_effected)]


"""def isCompanyExclusive(diskID: int) -> bool:
    def isCompanyExclusive(diskID: int) -> bool:
        conn = None
        try:
            conn = Connector.DBConnector()
            query = sql.SQL(
                # "SELECT D.D_id "
                # "FROM Disk D, DiskRam DR, Ram R "
                # "WHERE D.D_id = {disk_ID} "
                # "AND D.D_company = "
                # "ANY (SELECT S.R_Company FROM Ram S WHERE S.R_id = DR.R_id) "

                "SELECT D.D_id "
                "FROM Disk D "
                "WHERE D.D_id = {disk_ID} "
                "AND D.d_company = ALL (SELECT R.r_company FROM Ram R, DiskRam DR2 WHERE DR2.D_id={disk_ID} "
                "AND R.r_id = DR2.r_id);"
                # "OR D.D_company = ANY (SELECT R2.R_Company FROM Ram R2, Disk D2 WHERE R2.R_id = DR.R_id) )"
                # OR ram = empty.. NOT EXISTS?

            ).format(disk_ID=sql.Literal(diskID))

            rows_effected, res = conn.execute(query)
            conn.commit()
            if rows_effected == 0:
                conn.close()
                return False

        except DatabaseException.ConnectionInvalid:
            conn.close()
            return False
        except DatabaseException.database_ini_ERROR:
            conn.close()
            return False
        except Exception as e:
            "also catch te case no disk"
            # print("errorrrr: ")
            # print(e)
            conn.close()
            return False
        conn.close()
        return True"""

def isCompanyExclusive(diskID: int) -> bool:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            # "SELECT D.D_id "
            # "FROM Disk D, DiskRam DR, Ram R "
            # "WHERE D.D_id = {disk_ID} "
            # "AND D.D_company = "
            # "ANY (SELECT S.R_Company FROM Ram S WHERE S.R_id = DR.R_id) "

            "SELECT D.D_id "
            "FROM Disk D "
            "WHERE D.D_id = {disk_ID} "
            "AND D.d_company = ALL (SELECT R.r_company FROM Ram R, DiskRam DR2 WHERE DR2.D_id={disk_ID} "
            "AND R.r_id = DR2.r_id);"
            # "OR D.D_company = ANY (SELECT R2.R_Company FROM Ram R2, Disk D2 WHERE R2.R_id = DR.R_id) )"
            # OR ram = empty.. NOT EXISTS?

        ).format(disk_ID=sql.Literal(diskID))

        rows_effected, res = conn.execute(query)
        conn.commit()
        if rows_effected == 0:
            conn.close()
            return False

    except DatabaseException.ConnectionInvalid:
        conn.close()
        return False
    except DatabaseException.database_ini_ERROR:
        conn.close()
        return False
    except Exception as e:
        "also catch te case no disk"
        # print("errorrrr: ")
        # print(e)
        conn.close()
        return False
    conn.close()
    return True

def getConflictingDisks() -> List[int]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(  # "BEGIN;"
            "SELECT DISTINCT DQ1.D_id "
            "FROM DiskQuery DQ1, DiskQuery DQ2 "
            "WHERE DQ1.Q_id = DQ2.Q_id "
            "AND DQ1.D_id <> DQ2.D_id "
            "ORDER BY DQ1.D_id ASC;")

        rows_effected, res = conn.execute(query)
        conn.commit()

        if rows_effected == 0:
            conn.close()
            return list()
    except Exception as e:
        # print(e)
        conn.close()
        return list()

    conn.close()
    return [res[i]['D_id'] for i in range(0, rows_effected)]


def mostAvailableDisks() -> List[int]:
    conn = None
    try:
        conn = Connector.DBConnector()
        rows_effected, res = conn.execute(  # "BEGIN;"
            # "CREATE VIEW CanRunCount as"
            # " SELECT D.d_id, D.d_speed, (SELECT COUNT(*) "
            # "FROM CanRunOnDisk C WHERE C.d_id =D.d_id) as Count "
            # "FROM Disk D; "
            "SELECT d_id "
            "FROM CanRunCount "
            "ORDER BY Count DESC, d_speed DESC, d_id ASC "
            "LIMIT 5;")
        conn.commit()
        # print(res)

        if rows_effected == 0:
            conn.close()
            return list()
    except Exception as e:
        conn.close()
        return list()

    conn.close()
    return [res[i]['d_id'] for i in range(0, rows_effected)]

def getCloseQueries(queryID: int) -> List[int]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT DISTINCT Q.Q_id  "
            "FROM Query Q "
            "WHERE Q.Q_id <> {query_ID} "
            "AND ((SELECT COUNT(DISTINCT DQ1.D_id) "
                   "FROM DiskQuery DQ1, DiskQuery DQ2 "
                    "WHERE DQ1.D_id = DQ2.D_id "
                    "AND DQ1.Q_id = {query_ID} "
                    "AND DQ2.Q_id = Q.Q_id) "
                    ">=0.5*(SELECT COUNT(DISTINCT DQ.D_id) "
                            "FROM DiskQuery DQ "
                            "WHERE DQ.Q_id = {query_ID})) "
            "ORDER BY Q.Q_id ASC LIMIT 10;").format(query_ID=sql.Literal(queryID))

        rows_effected, res = conn.execute(query)
        conn.commit()
        if rows_effected == 0:
            conn.close()
            return list()
    except Exception as e:
        # print(e)
        conn.close()
        return list()

    conn.close()
    return [res[i]['Q_id'] for i in range(0, rows_effected)]



