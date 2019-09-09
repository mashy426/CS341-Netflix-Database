/*Program.cs*/

//
// Shyam Patel (NetID: spate54)
// U. of Illinois, Chicago
// CS 341, Fall 2018
// Project #06: Netflix database application
//

using System;
using System.Data;
using System.Data.SqlClient;

namespace program {
    class Program {
        //
        // connection info for Netflix database in Azure SQL
        //
        static string connectionInfo = String.Format(@"
        Server=tcp:jhummel2.database.windows.net,1433;Initial Catalog=Netflix;
        Persist Security Info=False;User ID=student;Password=cs341!uic;
        MultipleActiveResultSets=False;Encrypt=True;
        TrustServerCertificate=False;Connection Timeout=30;
        ");


        //
        // SQL query to retrieve top ten movies by average rating
        //
        static string TopTenMoviesQuery = String.Format(@"
        SELECT TOP 10
            dbo.Reviews.MovieID, COUNT(*) AS NumReviews,
            AVG(CONVERT(float,Rating)) AS AvgRating, MovieName
        FROM dbo.Reviews
        INNER JOIN dbo.Movies
            ON dbo.Reviews.MovieID = dbo.Movies.MovieID
        GROUP BY dbo.Reviews.MovieID, MovieName
        ORDER BY AvgRating DESC;
        ");


        //
        // GetMovieInfoQuery() : return SQL query to retrieve movie info
        //                       by movie ID or part of movie name
        //
        static string GetMovieInfoQuery(string _name) {
            string query = string.Format(@"
            SELECT
               dbo.Movies.MovieID, MovieName, MovieYear,
                COUNT(*) AS NumReviews,
                AVG(CONVERT(float,Rating)) AS AvgRating
            FROM dbo.Movies
            LEFT JOIN dbo.Reviews
                ON dbo.Movies.MovieID = dbo.Reviews.MovieID
            ");

            int _id;
            if (Int32.TryParse(_name, out _id)) {           // by movie ID
                query += string.Format(@"
                WHERE dbo.Movies.MovieID = " + _id + @"
                GROUP BY dbo.Movies.MovieID, MovieName, MovieYear;
                ");
            }
            else {                                          // by movie name
                _name  = _name.Replace("'", "''");
                query += string.Format(@"
                WHERE MovieName LIKE '%" + _name + @"%'
                GROUP BY dbo.Movies.MovieID, MovieName, MovieYear
                ORDER BY MovieName ASC;
                ");
            }

            return query;
        }//end GetMovieInfoQuery()


        //
        // GetUserInfoQuery() : return SQL query to retrieve user info
        //                      by user ID or part of user name
        //
        static string GetUserInfoQuery(string _name) {
            string query = string.Format(@"
            SELECT
                UserName, dbo.Users.UserID, Occupation,
                AVG(CONVERT(float,Rating)) AS AvgRating,
                COUNT(*) AS NumReviews,
                COUNT(CASE WHEN Rating = 1 THEN 1 END) AS OneStar,
                COUNT(CASE WHEN Rating = 2 THEN 1 END) AS TwoStars,
                COUNT(CASE WHEN Rating = 3 THEN 1 END) AS ThreeStars,
                COUNT(CASE WHEN Rating = 4 THEN 1 END) AS FourStars,
                COUNT(CASE WHEN Rating = 5 THEN 1 END) AS FiveStars
            FROM dbo.Users
            LEFT JOIN dbo.Reviews
                ON dbo.Users.UserID = dbo.Reviews.UserID
            ");

            int _id;
            if (Int32.TryParse(_name, out _id)) {           // by user ID
                query += string.Format(@"
                WHERE dbo.Users.UserID = " + _id + @"
                GROUP BY UserName, dbo.Users.UserID, Occupation;
                ");
            }
            else {                                          // by user name
                _name  = _name.Replace("'", "''");
                query += string.Format(@"
                WHERE UserName LIKE '%" + _name + @"%'
                GROUP BY UserName, dbo.Users.UserID, Occupation
                ORDER BY UserName ASC;
                ");
            }

            return query;
        }//end GetUserInfoQuery()


        //
        // TopTenMovies() : display top ten movies by average rating
        //
        static void TopTenMovies() {
            SqlConnection db = null;                        // SQL  connection
            try {
                db = new SqlConnection(connectionInfo);     // new  connection
                db.Open();                                  // open connection

                SqlCommand     cmd     = new SqlCommand();  // SQL command
                cmd.Connection         = db;                // set connection
                SqlDataAdapter adapter = new SqlDataAdapter(cmd);
                DataSet        ds      = new DataSet();     // data set
                cmd.CommandText        = TopTenMoviesQuery; // set cmd text
                adapter.Fill(ds);                           // retrieve rows
                var            rows    = ds.Tables["TABLE"].Rows;

                Console.WriteLine();
                if (rows.Count > 0)                         // data found
                    Console.WriteLine("Rank\tMovieID\tNumReviews\tAvgRating\tMovieName");
                else                                        // no data found
                    Console.WriteLine("** Data not found...");

                int rank = 0;
                foreach (DataRow row in rows) {             // output data
                    rank++;
                    int    movieID    = Convert.ToInt32 (row["MovieID"]);
                    int    numReviews = Convert.ToInt32 (row["NumReviews"]);
                    double avgRating  = Convert.ToDouble(row["AvgRating"]);
                    string movieName  = Convert.ToString(row["MovieName"]);
                    Console.WriteLine("{0}\t{1}\t{2}\t\t{3:0.00000}\t\t'{4}'",
                        rank, movieID, numReviews, avgRating, movieName);
                }
            } catch (Exception e) {                         // exception
                Console.WriteLine();
                Console.WriteLine("** Error: {0}", e.Message);
            } finally {
                if (db != null && db.State != ConnectionState.Closed)
                    db.Close();                             // close connection
            }
        }//end TopTenMovies()


        //
        // OutputMovieInfo() : display movie info by movie ID or movie name
        //
        static void OutputMovieInfo(string _name) {
            SqlConnection db = null;                        // SQL  connection
            try {
                db = new SqlConnection(connectionInfo);     // new  connection
                db.Open();                                  // open connection

                string sql = GetMovieInfoQuery(_name);      // get SQL query

                SqlCommand     cmd     = new SqlCommand();  // SQL command
                cmd.Connection         = db;                // set connection
                SqlDataAdapter adapter = new SqlDataAdapter(cmd);
                DataSet        ds      = new DataSet();     // data set
                cmd.CommandText        = sql;               // set cmd text
                adapter.Fill(ds);                           // retrieve rows
                var            rows    = ds.Tables["TABLE"].Rows;

                if (rows.Count == 0) {                      // movie not found
                    Console.WriteLine();
                    Console.WriteLine("** Movie not found...");
                }

                foreach (DataRow row in rows) {             // output data
                    int    movieID   = Convert.ToInt32 (row["MovieID"]);
                    string movieName = Convert.ToString(row["MovieName"]);
                    int    year      = Convert.ToInt32 (row["MovieYear"]);
                    Console.WriteLine();
                    Console.WriteLine("{0}", movieID);
                    Console.WriteLine("'{0}'", movieName);
                    Console.WriteLine("Year: {0}", year);

                    if (row["AvgRating"] != DBNull.Value) { // reviews found
                        int    numReviews = Convert.ToInt32 (row["NumReviews"]);
                        double avgRating  = Convert.ToDouble(row["AvgRating"]);
                        Console.WriteLine("Num reviews: {0}", numReviews);
                        Console.WriteLine("Avg rating: {0:0.00000}", avgRating);
                    }
                    else {                                  // no reviews found
                        Console.WriteLine("Num reviews: 0");
                        Console.WriteLine("Avg rating: N/A");
                    }
                }
            } catch (Exception e) {                         // exception
                Console.WriteLine();
                Console.WriteLine("** Error: {0}", e.Message);
            } finally {
                if (db != null && db.State != ConnectionState.Closed)
                    db.Close();                             // close connection
            }
        }//end OutputMovieInfo()


        //
        // OutputUserInfo() : display user info by user ID or user name
        //
        static void OutputUserInfo(string _name) {
            SqlConnection db = null;                        // SQL  connection
            try {
                db = new SqlConnection(connectionInfo);     // new  connection
                db.Open();                                  // open connection

                string sql = GetUserInfoQuery(_name);       // get SQL query

                SqlCommand     cmd     = new SqlCommand();  // SQL command
                cmd.Connection         = db;                // set connection
                SqlDataAdapter adapter = new SqlDataAdapter(cmd);
                DataSet        ds      = new DataSet();     // data set
                cmd.CommandText        = sql;               // set cmd text
                adapter.Fill(ds);                           // retrieve rows
                var            rows    = ds.Tables["TABLE"].Rows;

                if (rows.Count == 0) {                      // user not found
                    Console.WriteLine();
                    Console.WriteLine("** User not found...");
                }

                foreach (DataRow row in rows) {             // output data
                    string userName   = Convert.ToString(row["UserName"]);
                    int    userID     = Convert.ToInt32 (row["UserID"]);
                    string occupation = Convert.ToString(row["Occupation"]);
                    Console.WriteLine();
                    Console.WriteLine("{0}", userName);
                    Console.WriteLine("User id: {0}", userID);
                    Console.WriteLine("Occupation: {0}", occupation);

                    if (row["AvgRating"] != DBNull.Value) { // reviews found
                        double avgRating  = Convert.ToDouble(row["AvgRating"]);
                        int    numReviews = Convert.ToInt32 (row["NumReviews"]);
                        int    star1      = Convert.ToInt32 (row["OneStar"]);
                        int    star2      = Convert.ToInt32 (row["TwoStars"]);
                        int    star3      = Convert.ToInt32 (row["ThreeStars"]);
                        int    star4      = Convert.ToInt32 (row["FourStars"]);
                        int    star5      = Convert.ToInt32 (row["FiveStars"]);
                        Console.WriteLine("Avg rating: {0:0.00000}", avgRating);
                        Console.WriteLine("Num reviews: {0}", numReviews);
                        Console.WriteLine(" 1 star: {0}", star1);
                        Console.WriteLine(" 2 stars: {0}", star2);
                        Console.WriteLine(" 3 stars: {0}", star3);
                        Console.WriteLine(" 4 stars: {0}", star4);
                        Console.WriteLine(" 5 stars: {0}", star5);
                    }
                    else {                                  // no reviews found
                        Console.WriteLine("Avg rating: N/A");
                        Console.WriteLine("Num reviews: 0");
                    }
                }
            } catch (Exception e) {                         // exception
                Console.WriteLine();
                Console.WriteLine("** Error: {0}", e.Message);
            } finally {
                if (db != null && db.State != ConnectionState.Closed)
                    db.Close();                             // close connection
            }
        }//end OutputUserInfo()


        //
        // GetUserCommand() : return user command ('m', 't', 'u' or 'x')
        //
        static string GetUserCommand() {
            Console.WriteLine();
            Console.WriteLine("What would you like?");
            Console.WriteLine("m. movie info");
            Console.WriteLine("t. top-10 info");
            Console.WriteLine("u. user info");
            Console.WriteLine("x. exit");
            Console.Write    (">> ");
            string cmd = Console.ReadLine();

            return cmd.ToLower();
        } //end GetUserCommand()


        //
        // Main :
        //
        static void Main(string[] args) {
            Console.WriteLine("** Netflix Database App **");
            string cmd = GetUserCommand();                  // get command

            while (cmd != "x") {
                if (cmd == "m") {                           // movie info
                    Console.Write("Enter movie id or part of movie name>> ");
                    OutputMovieInfo(Console.ReadLine());
                }

                else if (cmd == "t")                        // top-10 info
                    TopTenMovies();

                else if (cmd == "u") {                      // user info
                    Console.Write("Enter user id or name>> ");
                    OutputUserInfo(Console.ReadLine());
                }

                cmd = GetUserCommand();                     // get next cmd
            }

            Console.WriteLine();
            Console.WriteLine("** Done **");
            Console.WriteLine();
        }//end Main()
    }//end Program class
}//end program namespace
