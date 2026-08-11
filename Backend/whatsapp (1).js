import React, { useState, useEffect } from 'react';
import io from 'socket.io-client';
import QRCode from 'react-qr-code';
import { FaCheckCircle, FaSpinner } from 'react-icons/fa'; // Icons for spinner and success tick

const socket = io.connect("https://goyee.onrender.com", {
    transports: ["polling", "websocket"]
});

export default function WhatsAppAuth() {
    const [qrCode, setQrCode] = useState("");
    const [isAuthenticated, setIsAuthenticated] = useState(false);

    useEffect(() => {
        socket.on("qr", (qr) => {
            setQrCode(qr);
        });

        socket.on("ready", (message) => {
            setIsAuthenticated(true);
        });
    }, []);

    return (
        <div className="flex flex-col items-center justify-center min-h-screen" style={{ backgroundColor: "#F0FDF4" }}>
            <div className="bg-white p-10 rounded-3xl shadow-2xl text-center max-w-md w-full border border-green-100 transition-all duration-500">
                <h2 className="text-3xl font-extrabold mb-8" style={{ color: "#25D366" }}>WhatsApp Link</h2>
                
                {isAuthenticated ? (
                    <div className="flex flex-col items-center">
                        <div className="rounded-full bg-green-100 p-4 mb-6 transition-transform duration-500 hover:scale-110">
                            <FaCheckCircle className="text-6xl text-green-500 animate-bounce" />
                        </div>
                        <h3 className="text-2xl text-gray-800 font-bold mb-3">Linked Successfully!</h3>
                        <p className="text-gray-500 mb-8">Your WhatsApp is now ready to send bulk messages.</p>
                        <button 
                            className="w-full py-3 px-6 text-white font-semibold rounded-xl shadow-lg transition-all hover:scale-105" 
                            style={{ backgroundColor: "#25D366" }}
                        >
                            Start Sending Messages
                        </button>
                    </div>
                ) : (
                    <div className="flex flex-col items-center">
                        {qrCode ? (
                            <div className="flex justify-center p-5 bg-white border-4 border-dashed rounded-2xl shadow-sm mb-6 transition-all duration-300 hover:shadow-md" style={{ borderColor: "#25D366" }}>
                                <QRCode value={qrCode} size={220} />
                            </div>
                        ) : (
                            <div className="flex flex-col items-center justify-center py-12">
                                <FaSpinner className="text-5xl animate-spin mb-4" style={{ color: "#25D366" }} />
                                <p className="text-gray-500 font-medium animate-pulse">Generating Secure QR Code...</p>
                            </div>
                        )}
                        <p className="text-sm text-gray-400 mt-2">Open WhatsApp &gt; Linked Devices &gt; Scan</p>
                    </div>
                )}
            </div>
        </div>
    );
}