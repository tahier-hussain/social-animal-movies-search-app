const express = require("express");
const router = express.Router();
const moviesSearchController = require("../../controllers/MoviesSearch");

router.get("/", moviesSearchController.search);

router.get("/index-data", moviesSearchController.indexData);

module.exports = router;
