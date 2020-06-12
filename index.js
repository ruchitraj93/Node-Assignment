const express = require("express");
const app = express();
const port = 3002;

var MongoClient = require("mongodb").MongoClient;
var url = "mongodb+srv://ruchitraj:FIW8H6wrY2DulPEO@cluster0-7ho5h.mongodb.net/Author_Ruchit?retryWrites=true&w=majority";
var moment = require("moment");

MongoClient.connect(url, function (err, db) {
  if (err) throw err;
  console.log("Database created!");

  //...fire the routes

  app.post("/book", function (req, res) {
    var myobj = [
      {
        authorid: 1,
        authorName: "Ruchit",
        awardNumber: 5,
        year: 2000,
        birthDate: moment("1998-12-16T10:00:00").format("YYYY-DD-MM"),
        books: [
          { bookName: "book1", price: 300, bookSold: 100 },
          { bookName: "book2", price: 250, bookSold: 50 },
          { bookName: "book3", price: 150, bookSold: 150 },
        ],
      },
      {
        authorid: 2,
        authorName: "Raj",
        awardNumber: 20,
        year: 2010,
        birthDate: moment("1993-06-21T10:00:00").format("YYYY-DD-MM"),
        books: [
          { bookName: "book1", price: 200, bookSold: 200 },
          { bookName: "book2", price: 300, bookSold: 100 },
          { bookName: "book3", price: 100, bookSold: 120 },
        ],
      },
      {
        authorid: 3,
        authorName: "random",
        awardNumber: 52,
        year: 2020,
        birthDate: moment("1963-06-06T10:00:00").format("YYYY-DD-MM"),
        books: [
          { bookName: "book1", price: 500, bookSold: 120 },
          { bookName: "book2", price: 400, bookSold: 300 },
          { bookName: "book3", price: 300, bookSold: 250 },
        ],
      },
    ];

    myobj.forEach((element) => {
      if (err) throw err;
      var dbo = db.db("Author_Ruchit");
      dbo.collection("Author_book").insertOne(element, function (err, res) {
        if (err) console.log("error", err); /* throw err; */
        console.log("1 document inserted");
      });
    });
  });

  app.get("/task1", function (req, res) {
    if (err) throw err;
    var dbo = db.db("Author_Ruchit");
    dbo
      .collection("Author_book")
      .find({ awardNumber: { $gte: Number(req.query.n) } })
      .toArray(function (err, result) {
        if (err) console.log("error", err); /* throw err; */
        res.send(result);
        console.log("record fetched: ", result);
      });
  });

  app.get("/task2", function (req, res) {
    console.log("n req", req);
    if (err) throw err;
    var dbo = db.db("Author_Ruchit");
    dbo
      .collection("Author_book")
      .find({ year: { $gte: Number(req.query.n) } })
      .toArray(function (err, result) {
        if (err) console.log("error", err); /* throw err; */
        res.send(result);
        console.log("record fetched: ", result);
      });
  });

  app.get("/task3", function (req, res) {
    console.log("n req", req);
    if (err) throw err;
    var dbo = db.db("Author_Ruchit");
    dbo
      .collection("Author_book")
      .aggregate([
        { $unwind: "$books" },
        {
          $group: {
            _id: { authorid: "$authorid", authorName: "$authorName" },
            totalBooksSold: { $sum: "$books.bookSold" },
            totalProfit: {
              $sum: { $multiply: ["$books.bookSold", "$books.price"] },
            },
          },
        },
      ])
      .toArray(function (err, result) {
        if (err) console.log("error", err); /* throw err; */
        res.send(result);
        console.log("record fetched: ", result);
      });
  });

  app.get("/task4", function (req, res) {
    console.log("n req", req);
    if (err) throw err;
    var dbo = db.db("Author_Ruchit");
    dbo
      .collection("Author_book")
      .aggregate([
        { $unwind: "$books" },
        {
          $match: {
            birthDate: { $gte: moment(req.query.n).format("YYYY-DD-MM") },
          },
        },
        {
          $group: {
            _id: { authorid: "$authorid", authorName: "$authorName" },
            totalBooksSold: { $sum: "$books.bookSold" },
            totalProfit: {
              $sum: { $multiply: ["$books.bookSold", "$books.price"] },
            },
          },
        },
        { $match: { totalProfit: { $gte: Number(req.query.m) } } },
      ])
      .toArray(function (err, result) {
        if (err) console.log("error", err); /* throw err; */
        res.send(result);
        console.log("record fetched: ", result);
      });
  });

  app.listen(port, () => {
    console.log("we are live on " + port);
  });
});
