const express = require("express");
const bodyParser = require("body-parser");
const app = express();
const cors = require("cors");
const path = require("path");

//Body Parser
app.use(bodyParser.json());
app.use(cors());

//Routes
app.use("/api/movies-search", require("./routes/api/MoviesSearch"));

const port = process.env.PORT || 5000;
const serveHost = process.env.YOUR_HOST || "0.0.0.0";

var server = app.listen(port, serveHost, () => {
  console.log(`Server running on ${port}`);
});
