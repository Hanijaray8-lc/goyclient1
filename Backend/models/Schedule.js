const mongoose = require('mongoose');

const scheduleSchema = new mongoose.Schema({
    email: { type: String, required: true },
    contacts: { type: [String], default: [] },
    message: { type: String, default: '' },
    media: { type: mongoose.Schema.Types.Mixed, default: null },
    scheduledFor: { type: Date, required: true },
    status: { type: String, enum: ['Pending', 'Completed', 'Failed'], default: 'Pending' }
}, { timestamps: true });

module.exports = mongoose.models.Schedule || mongoose.model('Schedule', scheduleSchema);

