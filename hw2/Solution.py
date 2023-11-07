from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Photo import Photo
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("""
        CREATE TABLE IF NOT EXISTS "Photo"
            (
                id integer NOT NULL PRIMARY KEY CHECK (id > 0),
                description TEXT NOT NULL,
                disk_size_needed integer NOT NULL CHECK (disk_size_needed >= 0)
            );
        CREATE TABLE IF NOT EXISTS "Disk"
            (
                id integer NOT NULL PRIMARY KEY CHECK (id > 0),
                manufacturing_company TEXT NOT NULL,
                speed integer NOT NULL CHECK (speed > 0),
                free_space integer NOT NULL CHECK (free_space >= 0),
                cost_per_byte integer NOT NULL CHECK (cost_per_byte > 0)
            );
         CREATE TABLE IF NOT EXISTS "RAM"
            (
                id integer NOT NULL PRIMARY KEY CHECK (id > 0),
                size integer NOT NULL CHECK (size > 0),
                company TEXT NOT NULL
            );

        CREATE TABLE IF NOT EXISTS "PhotoInDisk"
            (
                photo_id integer NOT NULL,
                disk_id integer NOT NULL,
                PRIMARY KEY (photo_id, disk_id),
                FOREIGN KEY (photo_id) REFERENCES "Photo" (id) ON DELETE CASCADE,
                FOREIGN KEY (disk_id) REFERENCES "Disk" (id) ON DELETE CASCADE
            );

        CREATE TABLE IF NOT EXISTS "RAMInDisk"
    		(
    			ram_id integer NOT NULL,
    			disk_id integer NOT NULL,
    			PRIMARY KEY (ram_id, disk_id),
    			FOREIGN KEY (ram_id) REFERENCES "RAM" (id) ON DELETE CASCADE,
    			FOREIGN KEY (disk_id) REFERENCES "Disk" (id) ON DELETE CASCADE
    		);

        CREATE OR REPLACE VIEW "TotalRAMInDisk" as 
        select "Disk".id as disk_id,COALESCE(SUM("RAM".size), 0) as total_ram
        from "Disk"
        left outer join "RAMInDisk" on "Disk".id = "RAMInDisk".disk_id
        left outer join  "RAM" on "RAM".id = "RAMInDisk".ram_id
        GROUP BY "Disk".id;

        CREATE OR REPLACE VIEW "DiskPhotoCounts" AS 
        SELECT 
            "Disk".id AS disk_id, 
            COALESCE(COUNT("Photo".id), 0) AS photo_count, 
            "Disk".speed AS disk_speed 
        FROM "Disk" 
        LEFT JOIN "Photo" ON "Disk".free_space >= "Photo".disk_size_needed
        GROUP BY "Disk".id, "Disk".speed;     
    """)
        conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()


def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("\n".join(['DELETE FROM "{table}";'.format(table=table) for table in ["Photo", "Disk", "RAM", "PhotoInDisk", "RAMInDisk"]]))
        conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("\n".join(['DROP TABLE IF EXISTS "{table}" CASCADE;'.format(table=table) for table in
               ["Photo", "Disk", "RAM", "PhotoInDisk", "RAMInDisk", "TotalRAMInDisk", "DiskPhotoCounts"]]))
        conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()

def addTuple(query) -> ReturnValue:
    conn = None
    result = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        conn.execute(query)
        conn.commit()
    except (DatabaseException.CHECK_VIOLATION, DatabaseException.NOT_NULL_VIOLATION):
        conn.rollback()
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        conn.rollback()
        result = ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION:
        conn.rollback()
        result = ReturnValue.ALREADY_EXISTS
    except Exception:
        conn.rollback()
        result = ReturnValue.ERROR
    finally:
        conn.close()
        return result

def addPhoto(photo: Photo) -> ReturnValue:
    return addTuple(sql.SQL('INSERT INTO "Photo" VALUES ({photo_id}, {description}, {disk_size_needed})').format(
        photo_id=sql.Literal(photo.getPhotoID()),
        description=sql.Literal(photo.getDescription()),
        disk_size_needed=sql.Literal(photo.getSize())
    ))


def getPhotoByID(photoID: int) -> Photo:
    conn = None
    result = Photo.badPhoto()
    try:
        conn = Connector.DBConnector()
        row_effected, entries = conn.execute(sql.SQL('SELECT * FROM "Photo" WHERE id = {id} ').format(id=sql.Literal(photoID)))
        if row_effected != 0:
            photo_id, description, size = entries[0].values()
            result.setPhotoID(photo_id)
            result.setDescription(description)
            result.setSize(size)
    except Exception as e:
        pass
    finally:
        conn.close()
        return result

def deleteTuple(query, not_photo=False):
    conn = None
    result = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        row_effected, entries = conn.execute(query)
        if row_effected != 0:
            conn.commit()
        else:
            if not_photo:
                result = ReturnValue.NOT_EXISTS
    except Exception as e:
        conn.rollback()
        result = ReturnValue.ERROR
    finally:
        conn.close()
        return result

def deletePhoto(photo: Photo) -> ReturnValue:
    return deleteTuple(sql.SQL(
        """
        UPDATE "Disk" SET free_space = free_space + {disk_size_needed} WHERE id IN
            (SELECT "PhotoInDisk".Disk_id FROM "PhotoInDisk" INNER JOIN "Photo" ON "Photo".id = "PhotoInDisk".photo_id  
                WHERE ("Photo".id, "Photo".description, "Photo".disk_size_needed) =
                                                                ({id}, {description}, {disk_size_needed}));          
        DELETE FROM "Photo" WHERE (id, description, disk_size_needed) =
                                                    ({id}, {description}, {disk_size_needed});
        """).format(
        id=sql.Literal(photo.getPhotoID()),
        description=sql.Literal(photo.getDescription()),
        disk_size_needed=sql.Literal(photo.getSize())))


def addDisk(disk: Disk) -> ReturnValue:
    return addTuple(sql.SQL("""
        INSERT INTO "Disk" (id, manufacturing_company, speed, free_space, cost_per_byte) 
        VALUES ({disk_id}, {manufacturing_company}, {speed}, {free_space}, {cost_per_byte});""").format(
        disk_id=sql.Literal(disk.getDiskID()),
        manufacturing_company=sql.Literal(disk.getCompany()),
        speed=sql.Literal(disk.getSpeed()),
        free_space=sql.Literal(disk.getFreeSpace()),
        cost_per_byte=sql.Literal(disk.getCost())
    ))


def getDiskByID(diskID: int) -> Disk:
    conn = None
    result = Disk.badDisk()
    try:
        conn = Connector.DBConnector()
        row_effected, entries = conn.execute(sql.SQL('SELECT * FROM "Disk" WHERE id = {id} ').format(id=sql.Literal(diskID)))
        if row_effected != 0:
            disk_id, manufacturing_company, speed, free_space, cost_per_byte = entries[0].values()
            result.setDiskID(disk_id)
            result.setCompany(manufacturing_company)
            result.setSpeed(speed)
            result.setFreeSpace(free_space)
            result.setCost(cost_per_byte)
    except Exception as e:
        pass
    finally:
        conn.close()
        return result


def deleteDisk(diskID: int) -> ReturnValue:
    return deleteTuple(query=sql.SQL('DELETE FROM "Disk" where id = {id}').format(id=sql.Literal(diskID)), not_photo=True)


def addRAM(ram: RAM) -> ReturnValue:
    return addTuple(sql.SQL('INSERT INTO "RAM" VALUES ({id}, {size}, {company})').format(
        id=sql.Literal(ram.getRamID()),
        size=sql.Literal(ram.getSize()),
        company=sql.Literal(ram.getCompany())
    ))


def getRAMByID(ramID: int) -> RAM:
    conn = None
    result = RAM.badRAM()
    try:
        conn = Connector.DBConnector()
        row_effected, entries = conn.execute(sql.SQL('SELECT * FROM "RAM" WHERE id = {id} ').format(id=sql.Literal(ramID)))
        if row_effected != 0:
            ram_id, size, company = entries[0].values()
            result.setRamID(ram_id)
            result.setCompany(company)
            result.setSize(size)
    except Exception as e:
        pass
    finally:
        conn.close()
        return result


def deleteRAM(ramID: int) -> ReturnValue:
    return deleteTuple(query=sql.SQL(
        'DELETE FROM "RAM" where id = {id}').format(
        id=sql.Literal(ramID)), not_photo=True)


def addDiskAndPhoto(disk: Disk, photo: Photo) -> ReturnValue:
    return addTuple(sql.SQL("""
        BEGIN TRANSACTION;
        INSERT INTO "Disk" VALUES ({disk_id}, {manufacturing_company}, {speed}, {free_space}, {cost_per_byte});
        INSERT INTO "Photo" VALUES ({photo_id}, {description}, {disk_size_needed});
        COMMIT;
        """).format(
        disk_id=sql.Literal(disk.getDiskID()),
        manufacturing_company=sql.Literal(disk.getCompany()),
        speed=sql.Literal(disk.getSpeed()),
        free_space=sql.Literal(disk.getFreeSpace()),
        cost_per_byte=sql.Literal(disk.getCost()),
        photo_id=sql.Literal(photo.getPhotoID()),
        description=sql.Literal(photo.getDescription()),
        disk_size_needed=sql.Literal(photo.getSize())
    ))


def addPhotoToDisk(photo: Photo, diskID: int) -> ReturnValue:
    conn = None
    result = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        conn.execute(sql.SQL("""
            BEGIN TRANSACTION;  
            INSERT INTO "PhotoInDisk" VALUES ((SELECT COALESCE("Photo".id) FROM "Photo" WHERE
            id = {photo_id} AND description = {photo_description} AND disk_size_needed = {photo_size}),
            (SELECT COALESCE("Disk".id) FROM "Disk" WHERE "Disk".id = {disk_id}));
            UPDATE "Disk" SET free_space = free_space - {photo_size} WHERE "Disk".id = {disk_id};
            COMMIT;
            """).format(
        photo_id=sql.Literal(photo.getPhotoID()),
        photo_description=sql.Literal(photo.getDescription()),
        photo_size=sql.Literal(photo.getSize()),
        disk_id=sql.Literal(diskID)))
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION:
        conn.rollback()
        result = ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION:
        conn.rollback()
        result = ReturnValue.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        conn.rollback()
        result = ReturnValue.BAD_PARAMS
    except Exception:
        conn.rollback()
        result = ReturnValue.ERROR
    finally:
        conn.close()
        return result


def removePhotoFromDisk(photo: Photo, diskID: int) -> ReturnValue:
    return deleteTuple(query=sql.SQL("""
        CREATE OR REPLACE VIEW "PhotoSize" AS
        SELECT COALESCE(
        (
            SELECT "Photo".disk_size_needed
            FROM "Photo"
            INNER JOIN "PhotoInDisk"
            ON "PhotoInDisk".disk_id = {diskID} AND "Photo".id = "PhotoInDisk".photo_id AND "Photo".id = {photoID}
        ), 0);

        UPDATE "Disk" set free_space=free_space + (SELECT * FROM "PhotoSize") where id = {diskID};
        DELETE FROM "PhotoInDisk" where Photo_id = {photoID} and disk_id = {diskID};
        """).format(
        photoID=sql.Literal(photo.getPhotoID()),
        PhotoSize=sql.Literal(photo.getSize()),
        diskID=sql.Literal(diskID)))


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    return addTuple(sql.SQL("""INSERT INTO "RAMInDisk" VALUES ({ram_id},{disk_id} )""").format(
        ram_id=sql.Literal(ramID),
        disk_id=sql.Literal(diskID)
    ))


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    return deleteTuple(query=sql.SQL("""
        DELETE FROM "RAMInDisk" where ram_id = {ramID} and disk_id = {diskID};
        """).format(
        ramID=sql.Literal(ramID),
        diskID=sql.Literal(diskID)), not_photo=True)


def averagePhotosSizeOnDisk(diskID: int) -> float:
    conn = None
    result = 0
    try:
        conn = Connector.DBConnector()
        row_effected, entries = conn.execute(sql.SQL("""
         SELECT COALESCE(       
         (SELECT AVG("Photo".disk_size_needed)
         FROM "Photo" INNER JOIN "PhotoInDisk" ON "PhotoInDisk".disk_id = {diskID} AND "Photo".id = "PhotoInDisk".photo_id)
         , 0);
    """).format(diskID=sql.Literal(diskID)))
        if row_effected != 0:
            result = entries.rows[0][0]
    except Exception as e:
        result = -1
    finally:
        conn.close()
    return result


def getTotalRamOnDisk(diskID: int) -> int:
    conn = None
    result = 0
    try:
        conn = Connector.DBConnector()
        row_effected, entries = conn.execute(sql.SQL("""
            select total_ram
            from "TotalRAMInDisk"
            where "TotalRAMInDisk".disk_id = {diskID}
        """).format(
        diskID=sql.Literal(diskID)))
        if row_effected != 0:
            result = entries.rows[0][0]
    except DatabaseException.ConnectionInvalid as e:
        return -1
    except Exception as e:
        print(e)
        return -1
    finally:
        conn.close()
    return result


def getCostForDescription(description: str) -> int:
    conn = None
    result = 0
    try:
        conn = Connector.DBConnector()
        row_effected, entries = conn.execute(sql.SQL("""
            SELECT COALESCE(
            (select sum("Disk".cost_per_byte * "Photo".disk_size_needed)
            from "Disk"
            inner join "PhotoInDisk" on "PhotoInDisk".disk_id = "Disk".id
            inner join "Photo" on "Photo".id = "PhotoInDisk".photo_id and "Photo".description = {description})
            , 0)
        """).format(
        description=sql.Literal(description)))
        if row_effected != 0:
            result = entries.rows[0][0]
    except Exception as e:
        result = -1
    finally:
        conn.close()
    return result


def getPhotosCanBeAddedToDisk(diskID: int) -> List[int]:
    conn = None
    result = []
    try:
        conn = Connector.DBConnector()
        row_effected, entries = conn.execute(sql.SQL("""
         SELECT "Photo".id FROM "Disk" INNER JOIN "Photo" ON "Photo".disk_size_needed <= "Disk".free_space 
         where "Disk".id = {disk_id} ORDER BY "Photo".id DESC LIMIT 5
        """).format(disk_id=sql.Literal(diskID)))
        for row in entries.rows:
            result.append(row[0])
    except Exception as e:
        result = []
    finally:
        conn.close()
    return result


def getPhotosCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    conn = None
    result = []
    try:
        conn = Connector.DBConnector()
        row_effected, entries = conn.execute(sql.SQL("""
            SELECT "Photo".id FROM "Disk" 
            LEFT OUTER JOIN "TotalRAMInDisk" ON "TotalRAMInDisk".disk_id="Disk".id
            LEFT OUTER JOIN "Photo" ON "Photo".disk_size_needed <= "Disk".free_space 
            AND "Photo".disk_size_needed <= "TotalRAMInDisk".total_ram
            WHERE "Disk".id = {disk_id} AND "Photo".id IS NOT NULL
            ORDER BY "Photo".id ASC 
            LIMIT 5;
        """).format(disk_id=sql.Literal(diskID)))
        for row in entries.rows:
            result.append(row[0])
    except Exception as e:
        result = []
    finally:
        conn.close()
    return result


def isCompanyExclusive(diskID: int) -> bool:
    conn = None
    result = False
    try:
        conn = Connector.DBConnector()
        rows_effected, entries = conn.execute(sql.SQL("""
            SELECT (COUNT(DISTINCT "RAM".company) = 1 
            AND MIN("Disk".manufacturing_company) = MIN("RAM".company)) 
            OR (COUNT(DISTINCT "RAM".company) = 0 AND EXISTS(SELECT "Disk".id FROM "Disk" WHERE "Disk".id = {disk_id})) 
            AS is_exclusive
            FROM "Disk"
            LEFT JOIN "RAMInDisk" ON "Disk".id = "RAMInDisk".disk_id
            LEFT JOIN "RAM" ON "RAMInDisk".ram_id = "RAM".id
            WHERE "Disk".id = {disk_id}
        """).format(disk_id=sql.Literal(diskID)))
        result = entries.rows[0][0]
    except Exception as e:
        pass
    finally:
        conn.close()
    return result


def isDiskContainingAtLeastNumExists(description : str, num : int) -> bool:
    conn = None
    result = False
    try:
        conn = Connector.DBConnector()
        _, results = conn.execute(sql.SQL("""
        SELECT EXISTS 
        (
            SELECT 1 FROM "PhotoInDisk"
            INNER JOIN "Photo" ON "Photo".id = "PhotoInDisk".photo_id
            WHERE "Photo".description = {description}
            GROUP BY "PhotoInDisk".disk_id
            HAVING COUNT(*) >= {num}
        ) AS result
        """).format(
            description=sql.Literal(description),
            num=sql.Literal(num)
        ))
        result = results.rows[0][0]
    except Exception as e:
        pass
    finally:
        conn.close()
    return result


def getDisksContainingTheMostData() -> List[int]:
    conn = None
    result = []
    try:
        conn = Connector.DBConnector()
        _, results = conn.execute(sql.SQL("""
            SELECT DISTINCT "Disk".id
            FROM "Disk"
            JOIN "PhotoInDisk" ON "Disk".id = "PhotoInDisk".disk_id
            JOIN "Photo" ON "PhotoInDisk".photo_id = "Photo".id
            GROUP BY "Disk".id
            ORDER BY SUM("Photo".disk_size_needed) DESC, "Disk".id ASC
            LIMIT 5;
        """))
        for row in results.rows:
            result.append(row[0])
    except Exception as e:
        pass
    finally:
        conn.close()
    return result


def getConflictingDisks() -> List[int]:
    conn = None
    result = []
    try:
        conn = Connector.DBConnector()
        _, results = conn.execute(sql.SQL("""
            SELECT DISTINCT p1.disk_id FROM "PhotoInDisk" AS p1 JOIN "PhotoInDisk" AS p2 ON p1.photo_id = p2.photo_id
            WHERE p1.disk_id <> p2.disk_id ORDER BY p1.disk_id ASC;
        """))
        for row in results.rows:
            result.append(row[0])
    except Exception as e:
        pass
    finally:
        conn.close()
    return result


def mostAvailableDisks() -> List[int]:
    conn = None
    result = []
    try:
        conn = Connector.DBConnector()
        _, results = conn.execute(sql.SQL("""
            SELECT disk_id 
            FROM "DiskPhotoCounts"
            ORDER BY photo_count DESC, disk_speed DESC, disk_id ASC 
            LIMIT 5;
        """))
        for row in results.rows:
            result.append(row[0])
    except Exception as e:
        pass
    finally:
        conn.close()
    return result


def getClosePhotos(photoID: int) -> List[int]:
    conn = None
    result = []
    try:
        conn = Connector.DBConnector()
        _, results = conn.execute(sql.SQL(""" 
            CREATE OR REPLACE VIEW "PhotoNotSavedOnSomeDisk" AS
            SELECT NOT EXISTS (SELECT * FROM "PhotoInDisk" WHERE "PhotoInDisk".photo_id = {photo_id});
            CREATE OR REPLACE VIEW "DisksPhotoSavedOn" AS
            SELECT "PhotoInDisk".disk_id FROM "PhotoInDisk" WHERE "PhotoInDisk".photo_id = {photo_id}; 
            (SELECT DISTINCT PID.photo_id FROM "PhotoInDisk" PID 
            WHERE PID.disk_id IN (SELECT * FROM "DisksPhotoSavedOn") AND PID.photo_id <> {photo_id}
            GROUP BY PID.photo_id
            HAVING COUNT(PID.photo_id) >= (SELECT COUNT(*) FROM "DisksPhotoSavedOn")  * 0.5 
            ORDER BY PID.photo_id ASC
            LIMIT 10)
            UNION ALL
            (SELECT "Photo".id FROM "Photo" WHERE (SELECT * FROM "PhotoNotSavedOnSomeDisk") AND "Photo".id <> {photo_id}
            ORDER BY "Photo".id ASC LIMIT 10);
        """).format(photo_id=sql.Literal(photoID)))
        for row in results.rows:
            result.append(row[0])
    except Exception as e:
        pass
    finally:
        conn.close()
    return result
