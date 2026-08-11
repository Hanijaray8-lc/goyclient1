const mongoose = require('mongoose');

const ScheduleSchema = new mongoose.Schema({
    email: { type: String },
    contacts: { type: [String], required: true },
    message: { type: String, required: true },
    media: { type: Object }, 
    scheduledFor: { type: Date, required: true },
    status: { type: String, enum: ['Pending', 'Completed', 'Failed', 'Cancelled'], default: 'Pending' },
    sentAt: { type: Date },
    createdAt: { type: Date, default: Date.now }
});

module.exports = mongoose.model('Schedule', ScheduleSchema);
