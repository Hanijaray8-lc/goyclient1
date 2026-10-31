const express = require('express');
const router = express.Router();

router.get('/', (req, res) => res.json({ success: true, campaigns: [] }));

module.exports = router;

