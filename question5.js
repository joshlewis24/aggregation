// movie-booking-api.js
// Run: node movie-booking-api.js
// npm: npm init -y && npm install express mongoose body-parser

const express = require("express");
const mongoose = require("mongoose");
const bodyParser = require("body-parser");

const app = express();
app.use(bodyParser.json());

// ---------- Mongo connection ----------
const MONGO_URL = process.env.MONGO_URL || "mongodb://127.0.0.1:27017/movieBookingDB";
mongoose
  .connect(MONGO_URL)
  .then(() => console.log("✅ Mongo connected"))
  .catch((err) => {
    console.error("❌ Mongo connection error:", err);
    process.exit(1);
  });

// ---------- Schemas & Models ----------
const movieSchema = new mongoose.Schema({
  _id: String, // e.g., "M1"
  title: String,
  genre: String,
  releaseYear: Number,
  durationMins: Number,
});

const userSchema = new mongoose.Schema({
  _id: String, // e.g., "U1"
  name: String,
  email: String,
  joinedAt: Date,
});

const bookingSchema = new mongoose.Schema({
  _id: String, // e.g., "B1"
  userId: String,
  movieId: String,
  bookingDate: Date,
  seats: Number,
  status: String, // "Booked", "Cancelled"
});

const Movie = mongoose.model("Movie", movieSchema);
const User = mongoose.model("User", userSchema);
const Booking = mongoose.model("Booking", bookingSchema);

// ---------- Create Routes ----------

// POST /movies -> create a movie
app.post("/movies", async (req, res) => {
  try {
    const payload = req.body;
    if (!payload._id || !payload.title) return res.status(400).send({ error: "_id and title required" });
    const doc = await Movie.create(payload);
    res.status(201).json(doc);
  } catch (err) {
    if (err.code === 11000) return res.status(409).json({ error: "Duplicate _id" });
    res.status(500).json({ error: err.message });
  }
});

// POST /users -> register a user
app.post("/users", async (req, res) => {
  try {
    const payload = req.body;
    if (!payload._id || !payload.name || !payload.email)
      return res.status(400).send({ error: "_id, name and email required" });
    payload.joinedAt = payload.joinedAt ? new Date(payload.joinedAt) : new Date();
    const doc = await User.create(payload);
    res.status(201).json(doc);
  } catch (err) {
    if (err.code === 11000) return res.status(409).json({ error: "Duplicate _id" });
    res.status(500).json({ error: err.message });
  }
});

// POST /bookings -> create a booking (only if user & movie exist)
app.post("/bookings", async (req, res) => {
  try {
    const payload = req.body;
    if (!payload._id || !payload.userId || !payload.movieId || !payload.seats)
      return res.status(400).send({ error: "_id, userId, movieId, seats required" });

    // Validate existence of user & movie
    const [user, movie] = await Promise.all([
      User.findById(payload.userId).lean(),
      Movie.findById(payload.movieId).lean(),
    ]);
    if (!user) return res.status(404).json({ error: "User not found" });
    if (!movie) return res.status(404).json({ error: "Movie not found" });

    payload.bookingDate = payload.bookingDate ? new Date(payload.bookingDate) : new Date();
    payload.status = payload.status || "Booked";

    const doc = await Booking.create(payload);
    res.status(201).json(doc);
  } catch (err) {
    if (err.code === 11000) return res.status(409).json({ error: "Duplicate _id" });
    res.status(500).json({ error: err.message });
  }
});

// ---------- Aggregation Routes ----------

// Route 1: /analytics/movie-bookings
// Get total bookings and total seats booked per movie
app.get("/analytics/movie-bookings", async (req, res) => {
  try {
    const pipeline = [
      // group by movieId to get counts and seat totals
      {
        $group: {
          _id: "$movieId",
          totalBookings: { $sum: 1 },
          totalSeats: { $sum: "$seats" },
        },
      },
      // lookup movie details
      {
        $lookup: {
          from: "movies",
          localField: "_id",
          foreignField: "_id",
          as: "movie",
        },
      },
      { $unwind: { path: "$movie", preserveNullAndEmptyArrays: true } },
      {
        $project: {
          _id: 0,
          movieId: "$_id",
          title: "$movie.title",
          genre: "$movie.genre",
          totalBookings: 1,
          totalSeats: 1,
        },
      },
      { $sort: { totalBookings: -1 } },
    ];

    const result = await Booking.aggregate(pipeline);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Route 2: /analytics/user-bookings
// Get booking history for each user with movie titles
app.get("/analytics/user-bookings", async (req, res) => {
  try {
    const pipeline = [
      // lookup user
      {
        $lookup: {
          from: "users",
          localField: "userId",
          foreignField: "_id",
          as: "user",
        },
      },
      { $unwind: "$user" },
      // lookup movie
      {
        $lookup: {
          from: "movies",
          localField: "movieId",
          foreignField: "_id",
          as: "movie",
        },
      },
      { $unwind: "$movie" },
      // project the booking entry with required fields
      {
        $project: {
          _id: 0,
          bookingId: "$_id",
          userId: "$user._id",
          userName: "$user.name",
          movieId: "$movie._id",
          movieTitle: "$movie.title",
          bookingDate: "$bookingDate",
          seats: "$seats",
          status: "$status",
        },
      },
      // group by user to build booking arrays
      {
        $group: {
          _id: { userId: "$userId", userName: "$userName" },
          bookings: {
            $push: {
              bookingId: "$bookingId",
              movieId: "$movieId",
              movieTitle: "$movieTitle",
              bookingDate: "$bookingDate",
              seats: "$seats",
              status: "$status",
            },
          },
          totalBookings: { $sum: 1 },
        },
      },
      // reshape
      {
        $project: {
          _id: 0,
          userId: "$_id.userId",
          userName: "$_id.userName",
          totalBookings: 1,
          bookings: 1,
        },
      },
      { $sort: { totalBookings: -1 } },
    ];

    const result = await Booking.aggregate(pipeline);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Route 3: /analytics/top-users
// Find users who booked more than 2 times
app.get("/analytics/top-users", async (req, res) => {
  try {
    const pipeline = [
      { $group: { _id: "$userId", totalBookings: { $sum: 1 } } },
      { $match: { totalBookings: { $gt: 2 } } },
      {
        $lookup: {
          from: "users",
          localField: "_id",
          foreignField: "_id",
          as: "user",
        },
      },
      { $unwind: "$user" },
      {
        $project: {
          _id: 0,
          userId: "$_id",
          name: "$user.name",
          email: "$user.email",
          totalBookings: 1,
        },
      },
      { $sort: { totalBookings: -1 } },
    ];

    const result = await Booking.aggregate(pipeline);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Route 4: /analytics/genre-wise-bookings
// Total seats booked per genre
app.get("/analytics/genre-wise-bookings", async (req, res) => {
  try {
    const pipeline = [
      // join movies to get genre for each booking
      {
        $lookup: {
          from: "movies",
          localField: "movieId",
          foreignField: "_id",
          as: "movie",
        },
      },
      { $unwind: "$movie" },
      // group by genre and sum seats
      {
        $group: {
          _id: "$movie.genre",
          totalSeatsBooked: { $sum: "$seats" },
          bookingsCount: { $sum: 1 },
        },
      },
      {
        $project: {
          _id: 0,
          genre: "$_id",
          totalSeatsBooked: 1,
          bookingsCount: 1,
        },
      },
      { $sort: { totalSeatsBooked: -1 } },
    ];

    const result = await Booking.aggregate(pipeline);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Route 5: /analytics/active-bookings
// Get all current active ("Booked") bookings with movie and user details
app.get("/analytics/active-bookings", async (req, res) => {
  try {
    const pipeline = [
      { $match: { status: "Booked" } },
      {
        $lookup: {
          from: "users",
          localField: "userId",
          foreignField: "_id",
          as: "user",
        },
      },
      { $unwind: "$user" },
      {
        $lookup: {
          from: "movies",
          localField: "movieId",
          foreignField: "_id",
          as: "movie",
        },
      },
      { $unwind: "$movie" },
      {
        $project: {
          _id: 0,
          bookingId: "$_id",
          bookingDate: 1,
          seats: 1,
          status: 1,
          user: {
            userId: "$user._id",
            name: "$user.name",
            email: "$user.email",
          },
          movie: {
            movieId: "$movie._id",
            title: "$movie.title",
            genre: "$movie.genre",
          },
        },
      },
      { $sort: { bookingDate: -1 } },
    ];

    const result = await Booking.aggregate(pipeline);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ---------- Simple health route ----------
app.get("/", (req, res) => res.send({ ok: true, msg: "Movie Booking API running" }));

// ---------- Start server ----------
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
