const express = require("express");
const router = express.Router();
const moviesSearchController = require("../../controllers/MoviesSearch");

router.post("/", moviesSearchController.search);

module.exports = router;
