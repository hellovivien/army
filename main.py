import mysql.connector as mysql
from faker import Faker
from prettytable import PrettyTable
import random


class Database:
    def __init__(self, db_name):
        self._conn = mysql.connect(
        host = "localhost",
        user = "python",
        passwd = "password",
        database = db_name
    )
        self._cursor = self._conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def connection(self):
        return self._conn

    @property
    def cursor(self):
        return self._cursor

    def commit(self):
        self.connection.commit()

    def close(self, commit=True):
        if commit:
            self.commit()
        self.connection.close()

    def execute(self, sql, params=None):
        self.cursor.execute(sql, params or ())

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def query(self, sql, params=None):
        self.cursor.execute(sql, params or ())
        return self.fetchall()


def reset(db_name = None):
    system_db = ("information_schema", "mysql", "performance_schema", "sys")    
    conn = mysql.connect(
        host = "localhost",
        user = "python",
        passwd = "password"
    )
    cursor = conn.cursor()
    cursor.execute("SHOW DATABASES")
    for db, in cursor.fetchall():
        if (db not in system_db and not db_name) or db == db_name :
            cursor.execute(f"DROP DATABASE {db}")
    cursor.execute(f"CREATE DATABASE {db_name}")
    print(f"Database {db_name} has been reseted")
    conn.close()

def get_final_score(score, level, bonus, score_min):
    if level == "easy":
        bonus = 0
    elif level == "hard":
        bonus *= 2
    score += bonus
    return int(score >= score_min) * score


if __name__ == "__main__":

    fake = Faker("FR-fr")


    """
    CREATE TABLES
    """



    reset("army")
    with Database("army") as db:
        db.execute("""CREATE TABLE soldier (
            reg_number INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            first_name VARCHAR(255) NOT NULL,
            last_name VARCHAR(255) NOT NULL,
            email VARCHAR(255),
            grade VARCHAR(20),
            is_instructor BOOL)
        """)
        db.execute("""
            CREATE TABLE obstacle(
                id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                level VARCHAR(20),
                score_mini INT,
                bonus INT)
        """)
        db.execute("""
            CREATE TABLE test (
                id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                soldier_id INT NOT NULL,
                instructor_id INT,
                obstacle_id INT,
                score INT NOT NULL,
                duration INT not null,
                date_test DATE,
                FOREIGN KEY (soldier_id) REFERENCES soldier(reg_number) ON DELETE CASCADE,
                FOREIGN KEY (instructor_id) REFERENCES soldier(reg_number) ON DELETE SET NULL,
                FOREIGN KEY (obstacle_id) REFERENCES obstacle(id) ON DELETE SET NULL)
        """)



        """
        POPULATE DATABASE
        """


        GRADES = ["officier", "soldat", "sous-officier", "commandant"]
        LEVELS = ["easy", "medium", "hard"]
        TOTAL_SOLDIER = 10
        TOTAL_INSTRUCTOR = 3
        TOTAL_OBSTACLE = 5

        for i in range(1, TOTAL_OBSTACLE+1):
            obstacle = (f"obstacle{i}", random.choice(LEVELS), random.randint(1, 5), 1)
            db.execute("INSERT INTO obstacle (name, level, score_mini, bonus) VALUES (%s, %s, %s, %s)", obstacle)

        for i in range(TOTAL_SOLDIER + TOTAL_INSTRUCTOR):
            soldier = (fake.first_name(), fake.last_name(), fake.email(), random.choice(GRADES), i < TOTAL_INSTRUCTOR)
            db.execute("INSERT INTO soldier (first_name, last_name, email, grade, is_instructor) VALUES (%s, %s, %s, %s, %s)", soldier)
            if i >= TOTAL_INSTRUCTOR:
                test = (i+1, random.randint(1, TOTAL_INSTRUCTOR), random.randint(1, TOTAL_OBSTACLE), random.randint(1,5), random.randint(10, 120), fake.date_between(start_date='-20y', end_date='-1d'))
                db.execute("INSERT INTO test (soldier_id, instructor_id, obstacle_id, score, duration, date_test) VALUES (%s, %s, %s, %s, %s, %s)", test)

        """
        PRINT DATABASE
        """
        print("OBSTACLES")
        for obstacle in db.query("SELECT * FROM obstacle"):
            print(obstacle)
        print("INSTRUCTORS")
        for instructor in db.query("SELECT * FROM soldier WHERE is_instructor = 1"):
            print(instructor)
        print("SOLDIERS")
        for soldier in db.query("SELECT * FROM soldier  WHERE is_instructor = 0"):
            print(soldier)
        print("TEST")
        for test in db.query("SELECT * FROM test"):
            print(test)
        
        query = """
        SELECT
            CONCAT('[',s.reg_number,'] ', s.first_name, ' ', s.last_name),
            CONCAT('[',i.reg_number,'] ', i.first_name, ' ', i.last_name),
            o.name, o.level, o.bonus, o.score_mini, t.score, t.duration, t.date_test
        FROM test as t
        INNER JOIN soldier AS s ON t.soldier_id = s.reg_number
        LEFT JOIN (SELECT * FROM soldier WHERE is_instructor = 1) AS i ON t.instructor_id = i.reg_number
        LEFT JOIN obstacle AS o ON t.obstacle_id = o.id
        """


        print("TEST WITH JOINTURES")

        def print_tests():
            x = PrettyTable()
            x.field_names = ["Soldat", "Instructeur", "Obstacle", "Level", "Bonus", "Score Mini", "Score", "Temps", "Date", "SCORE FINAL"]
            for test in db.query(query):
                
                # print(test)
                final_score = get_final_score(test[6], test[3], test[4], test[5]) or "ECHEC"
                x.add_row(test + (final_score, ))
            print(x)
        
        print_tests()
        
        


        """
        COMMANDS
        """

        
        some_cmd = [
            "UPDATE soldier SET first_name = 'Nicolas' WHERE reg_number = 5",
            "DELETE from soldier WHERE reg_number = 10", #cascade
            "DELETE from obstacle WHERE id = 1" #set null
        ]
        run = True
        while run:
            cmd = input("commande : ")
            if cmd:
                if cmd[0:6].lower() == "select":
                    for item in db.query(cmd):
                        print(item)
                else:
                    db.execute(cmd)
                    print_tests()
            else:
                for cmd in some_cmd:
                    print(cmd)
                    db.execute(cmd)
                    print_tests()