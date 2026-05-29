import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:pedometer/pedometer.dart';
import 'package:permission_handler/permission_handler.dart';
import 'package:wakelock_plus/wakelock_plus.dart';
import 'package:sensors_plus/sensors_plus.dart';
import 'package:flutter_phone_direct_caller/flutter_phone_direct_caller.dart';
import 'package:firebase_core/firebase_core.dart';
import 'package:firebase_auth/firebase_auth.dart';
import 'package:cloud_firestore/cloud_firestore.dart';
import 'dart:async';
import 'dart:math';

void main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await Firebase.initializeApp();
  runApp(const BioCycleWatchApp());
}

class BioCycleWatchApp extends StatelessWidget {
  const BioCycleWatchApp({super.key});
  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      debugShowCheckedModeBanner: false,
      theme: ThemeData.dark().copyWith(scaffoldBackgroundColor: Colors.black),
      home: const WatchLoginScreen(),
    );
  }
}

// ==========================================
// ECRAN LOGIN CEAS — identic cu ce aveai
// ==========================================
class WatchLoginScreen extends StatefulWidget {
  const WatchLoginScreen({super.key});
  @override
  State<WatchLoginScreen> createState() => _WatchLoginScreenState();
}

class _WatchLoginScreenState extends State<WatchLoginScreen> {
  final _emailCtrl = TextEditingController();
  final _passCtrl = TextEditingController();
  bool _isLoading = false;
  String _error = '';

  Future<void> _login() async {
    setState(() { _isLoading = true; _error = ''; });
    try {
      await FirebaseAuth.instance.signInWithEmailAndPassword(
        email: _emailCtrl.text.trim(),
        password: _passCtrl.text,
      );
      if (mounted) {
        Navigator.pushReplacement(context, MaterialPageRoute(builder: (_) => const WatchSensorScreen()));
      }
    } catch (_) {
      setState(() { _error = 'Email/parolă greșită'; });
    }
    if (mounted) setState(() => _isLoading = false);
  }

  @override
  Widget build(BuildContext context) {
    if (FirebaseAuth.instance.currentUser != null) {
      WidgetsBinding.instance.addPostFrameCallback((_) {
        Navigator.pushReplacement(context, MaterialPageRoute(builder: (_) => const WatchSensorScreen()));
      });
    }

    return Scaffold(
      body: Center(
        child: SingleChildScrollView(
          padding: const EdgeInsets.all(16),
          child: Column(
            mainAxisAlignment: MainAxisAlignment.center,
            children: [
              const Icon(Icons.monitor_heart, color: Colors.greenAccent, size: 28),
              const SizedBox(height: 8),
              const Text('BIOCYCLE', style: TextStyle(color: Colors.greenAccent, fontSize: 14, fontWeight: FontWeight.bold, letterSpacing: 2)),
              const SizedBox(height: 16),
              TextField(
                controller: _emailCtrl,
                style: const TextStyle(color: Colors.white, fontSize: 11),
                keyboardType: TextInputType.emailAddress,
                decoration: InputDecoration(
                  hintText: 'Email',
                  hintStyle: TextStyle(color: Colors.grey[600], fontSize: 11),
                  filled: true, fillColor: Colors.grey[900],
                  border: OutlineInputBorder(borderRadius: BorderRadius.circular(10), borderSide: BorderSide.none),
                  contentPadding: const EdgeInsets.symmetric(horizontal: 10, vertical: 8),
                ),
              ),
              const SizedBox(height: 8),
              TextField(
                controller: _passCtrl,
                obscureText: true,
                style: const TextStyle(color: Colors.white, fontSize: 11),
                decoration: InputDecoration(
                  hintText: 'Parolă',
                  hintStyle: TextStyle(color: Colors.grey[600], fontSize: 11),
                  filled: true, fillColor: Colors.grey[900],
                  border: OutlineInputBorder(borderRadius: BorderRadius.circular(10), borderSide: BorderSide.none),
                  contentPadding: const EdgeInsets.symmetric(horizontal: 10, vertical: 8),
                ),
              ),
              const SizedBox(height: 12),
              if (_error.isNotEmpty)
                Text(_error, style: const TextStyle(color: Colors.redAccent, fontSize: 10)),
              const SizedBox(height: 4),
              _isLoading
                  ? const CircularProgressIndicator(color: Colors.greenAccent, strokeWidth: 2)
                  : SizedBox(
                      width: double.infinity,
                      child: ElevatedButton(
                        style: ElevatedButton.styleFrom(backgroundColor: Colors.greenAccent, foregroundColor: Colors.black),
                        onPressed: _login,
                        child: const Text('INTRĂ', style: TextStyle(fontWeight: FontWeight.bold, fontSize: 12)),
                      ),
                    ),
            ],
          ),
        ),
      ),
    );
  }
}

// ==========================================
// ECRAN PRINCIPAL CEAS
// ==========================================
class WatchSensorScreen extends StatefulWidget {
  const WatchSensorScreen({super.key});
  @override
  State<WatchSensorScreen> createState() => _WatchSensorScreenState();
}

class _WatchSensorScreenState extends State<WatchSensorScreen> {
  // ==========================================
  // FIX BUG PEDOMETRU — Double Buffer System
  // Problema: când apare o alertă, senzorul
  // îngheață și adaugă pașii acumulați brusc.
  // Soluția: buffer separat care numără în
  // spate chiar și când UI-ul e blocat.
  // ==========================================
  StreamSubscription<StepCount>? _stepCountStream;

  // Buffer A: pași acumulați înainte de alertă
  int _accumulatedSteps = 0;
  // Buffer B: referința curentă a senzorului
  int? _referenceSteps;
  // Pași afișați în UI (combinația celor două buffere)
  int _displaySteps = 0;
  // Flag: senzorul e "înghețat" din cauza unei alerte
  bool _isSensorFrozen = false;
  // Ultimul raw step count recepționat (chiar dacă UI e blocat)
  int _lastRawSteps = 0;

  static const heartRateChannel = EventChannel('biocycle.watch/heart_rate');
  StreamSubscription? _heartRateSubscription;
  StreamSubscription<AccelerometerEvent>? _accelSubscription;
  Timer? _workoutTimer;

  bool _isFallDialogActive = false;
  int _displayHr = 0;
  int _sumHr = 0;
  int _hrReadingsCount = 0;
  int _durationSeconds = 0;

  bool _isRunning = false;
  bool _isPaused = false;
  bool _hasAlertedHighHr = false;

  String _selectedSport = 'Alergare';
  final List<String> _sports = ['Alergare', 'Ciclism', 'Înot', 'Workout'];

  int _chronoAge = 22;

  // NOU: Listener pentru sesiune pornită de pe telefon
  StreamSubscription<DocumentSnapshot>? _phoneSessionListener;

  @override
  void initState() {
    super.initState();
    WidgetsBinding.instance.addPostFrameCallback((_) {
      _requestPermissions();
      _loadChronoAge();
      _listenForPhoneSession(); // NOU: ascultă dacă telefonul pornește sesiunea
    });
  }

  Future<void> _loadChronoAge() async {
    final uid = FirebaseAuth.instance.currentUser?.uid;
    if (uid == null) return;
    try {
      final doc = await FirebaseFirestore.instance.collection('users').doc(uid).get();
      if (doc.exists && mounted) {
        setState(() => _chronoAge = ((doc.data()?['age'] ?? 22) as num).toInt());
      }
    } catch (_) {}
  }

  Future<void> _requestPermissions() async {
    await [Permission.activityRecognition, Permission.sensors].request();
    if (mounted) setState(() {});
  }

  // ==========================================
  // NOU: Ascultă dacă telefonul pornește sesiunea
  // Dacă da, pornim automat și pe ceas
  // ==========================================
  void _listenForPhoneSession() {
    final uid = FirebaseAuth.instance.currentUser?.uid;
    if (uid == null) return;

    _phoneSessionListener = FirebaseFirestore.instance
        .collection('users')
        .doc(uid)
        .collection('active_session')
        .doc('current')
        .snapshots()
        .listen((snap) {
      if (!snap.exists || !mounted) return;
      final data = snap.data() as Map<String, dynamic>;
      final isActive = data['isActive'] as bool? ?? false;
      final startedBy = data['startedBy'] as String? ?? '';
      final sport = data['sportName'] as String? ?? 'Alergare';

      // Dacă telefonul a pornit sesiunea și ceasul nu rulează încă
      if (isActive && startedBy == 'phone' && !_isRunning) {
        setState(() => _selectedSport = sport);
        _startWorkout();
      }
    });
  }

  void _startWorkout() async {
    setState(() {
      _isRunning = true;
      _isPaused = false;
      _displaySteps = 0;
      _accumulatedSteps = 0;
      _referenceSteps = null;
      _lastRawSteps = 0;
      _isSensorFrozen = false;
      _sumHr = 0;
      _hrReadingsCount = 0;
      _durationSeconds = 0;
      _hasAlertedHighHr = false;
    });
    try { await WakelockPlus.enable(); } catch (_) {}
    _startSensors();

    // Publicăm în Firestore că ceasul a pornit (ceasul = master)
    _publishWatchSession();

    _workoutTimer = Timer.periodic(const Duration(seconds: 1), (timer) {
      if (!mounted) return;
      if (!_isPaused) setState(() => _durationSeconds++);
    });
  }

  // ==========================================
  // NOU: Publică datele live în Firestore
  // Telefonul le citește și se sincronizează
  // ==========================================
  Future<void> _publishWatchSession() async {
    final uid = FirebaseAuth.instance.currentUser?.uid;
    if (uid == null) return;
    try {
      await FirebaseFirestore.instance
          .collection('users')
          .doc(uid)
          .collection('active_session')
          .doc('current')
          .set({
        'isActive': true,
        'sportName': _selectedSport,
        'startedBy': 'watch',
        'startedAt': Timestamp.now(),
        'hr': _displayHr,
        'steps': _displaySteps,
      });
    } catch (_) {}
  }

  // Actualizează datele live în Firestore la fiecare 2 secunde
  Future<void> _updateWatchLiveData() async {
    final uid = FirebaseAuth.instance.currentUser?.uid;
    if (uid == null) return;
    try {
      await FirebaseFirestore.instance
          .collection('users')
          .doc(uid)
          .collection('active_session')
          .doc('current')
          .update({'hr': _displayHr, 'steps': _displaySteps});
    } catch (_) {}
  }

  void _pauseWorkout() {
    setState(() {
      _isPaused = true;
      // Salvăm pașii acumulați în buffer A
      _accumulatedSteps = _displaySteps;
      // Resetăm referința pentru a evita saltul la reluare
      _referenceSteps = null;
    });
  }

  void _resumeWorkout() {
    setState(() {
      _isPaused = false;
      // Buffer-ul va fi resetat la primul pas recepționat
      _referenceSteps = null;
    });
  }

  void _startSensors() {
    if (_selectedSport == 'Alergare') {
      _stepCountStream = Pedometer.stepCountStream.listen((event) {
        if (!mounted) return;

        // Stocăm mereu ultimul raw count, chiar dacă UI e blocat
        _lastRawSteps = event.steps;

        if (_isPaused) return;

        // ==========================================
        // FIX DOUBLE BUFFER:
        // Când senzorul revine după o alertă,
        // resetăm referința ca să nu sară pașii
        // ==========================================
        if (_isSensorFrozen) {
          // Senzorul tocmai s-a deblocat — resetăm referința
          _referenceSteps = event.steps;
          _isSensorFrozen = false;
        }

        _referenceSteps ??= event.steps;
        final currentSegmentSteps = event.steps - _referenceSteps!;

        if (!_isFallDialogActive) {
          setState(() => _displaySteps = _accumulatedSteps + currentSegmentSteps);
        } else {
          // Continuăm să numărăm în buffer, fără a actualiza UI
          _displaySteps = _accumulatedSteps + currentSegmentSteps;
        }
      }, onError: (_) {});
    }

    _heartRateSubscription = heartRateChannel.receiveBroadcastStream().listen((event) {
      if (!mounted || _isPaused) return;

      final rawHr = (event as double).toInt();
      if (rawHr > 30 && rawHr < 220) {
        _sumHr += rawHr;
        _hrReadingsCount++;

        if (!_isFallDialogActive) {
          setState(() => _displayHr = rawHr);
          // Actualizăm datele live pe Firestore
          _updateWatchLiveData();
        }

        if (rawHr >= 120 && !_hasAlertedHighHr) {
          _hasAlertedHighHr = true;
          // Marcăm că urmează o alertă — senzorul se va "dezgheța" după
          _isSensorFrozen = true;
          _showHighHrAlert();
        }
      }
    });

    _accelSubscription = accelerometerEventStream().listen((event) {
      if (!mounted || _isFallDialogActive) return;
      final gForce = sqrt(event.x * event.x + event.y * event.y + event.z * event.z);
      if (gForce > 80.0) {
        _isSensorFrozen = true; // Marcăm înghețul înaintea alertei
        _triggerFallAlert();
      }
    });
  }

  void _showHighHrAlert() {
    showDialog(
      context: context,
      barrierDismissible: false,
      builder: (_) => AlertDialog(
        backgroundColor: Colors.black.withValues(alpha: 0.9),
        shape: RoundedRectangleBorder(
            side: const BorderSide(color: Colors.orangeAccent, width: 2),
            borderRadius: BorderRadius.circular(15)),
        content: SingleChildScrollView(
          child: Padding(
            padding: const EdgeInsets.symmetric(vertical: 8),
            child: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                const Icon(Icons.monitor_heart, color: Colors.orangeAccent, size: 28),
                const SizedBox(height: 8),
                const Text('PULS RIDICAT!', textAlign: TextAlign.center,
                    style: TextStyle(color: Colors.orangeAccent, fontWeight: FontWeight.bold, fontSize: 14)),
                const SizedBox(height: 6),
                const Text('Ai depășit limita de\n120 BPM setată.', textAlign: TextAlign.center,
                    style: TextStyle(fontSize: 12, color: Colors.white, height: 1.2)),
                const SizedBox(height: 12),
                ElevatedButton(
                  style: ElevatedButton.styleFrom(
                    backgroundColor: Colors.orangeAccent, foregroundColor: Colors.black,
                    padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 5),
                    minimumSize: const Size(60, 30),
                  ),
                  onPressed: () {
                    // La închiderea alertei, resetăm buffer-ul
                    _isSensorFrozen = true;
                    _referenceSteps = _lastRawSteps;
                    Navigator.pop(context);
                  },
                  child: const Text('AM ÎNȚELES', style: TextStyle(fontSize: 10, fontWeight: FontWeight.bold)),
                ),
              ],
            ),
          ),
        ),
      ),
    ).then((_) {
      // Deblocăm senzorul după ce dialogul s-a închis
      if (mounted) {
        setState(() {
          _isSensorFrozen = false;
          _referenceSteps = _lastRawSteps;
          _accumulatedSteps = _displaySteps;
        });
      }
    });
  }

  void _triggerFallAlert() async {
    _isFallDialogActive = true;

    // ==========================================
    // NOU: Zgomot sonor la alertă de cădere
    // Folosim HapticFeedback + SystemSound
    // ==========================================
    HapticFeedback.vibrate();
    await Future.delayed(const Duration(milliseconds: 300));
    HapticFeedback.heavyImpact();
    await Future.delayed(const Duration(milliseconds: 300));
    HapticFeedback.heavyImpact();
    SystemSound.play(SystemSoundType.alert);

    final needsHelp = await showGeneralDialog<bool>(
      context: context,
      barrierDismissible: false,
      pageBuilder: (_, __, ___) => const WatchFallDialog(),
    );

    _isFallDialogActive = false;

    // Deblocăm senzorul după alertă — resetăm referința
    if (mounted) {
      setState(() {
        _isSensorFrozen = false;
        _referenceSteps = _lastRawSteps;
        _accumulatedSteps = _displaySteps;
      });
    }

    if (needsHelp == true) _makeCall();
  }

  void _makeCall() async {
    const String emergencyNumber = '+40743430619';
    await FlutterPhoneDirectCaller.callNumber(emergencyNumber);
  }

  void _stopWorkout() {
    _workoutTimer?.cancel();
    _stepCountStream?.cancel();
    _heartRateSubscription?.cancel();
    _accelSubscription?.cancel();
    _phoneSessionListener?.cancel();
    WakelockPlus.disable();

    // Curățăm sesiunea activă
    final uid = FirebaseAuth.instance.currentUser?.uid;
    if (uid != null) {
      FirebaseFirestore.instance
          .collection('users')
          .doc(uid)
          .collection('active_session')
          .doc('current')
          .set({'isActive': false});
    }

    final finalAvg = _hrReadingsCount > 0 ? (_sumHr / _hrReadingsCount).round() : 0;

    Navigator.pushReplacement(context, MaterialPageRoute(
      builder: (_) => WatchSummaryScreen(
        sport: _selectedSport,
        steps: _displaySteps,
        avgHr: finalAvg,
        chronoAge: _chronoAge,
        durationSeconds: _durationSeconds,
      ),
    ));
  }

  @override
  void dispose() {
    _workoutTimer?.cancel();
    _stepCountStream?.cancel();
    _heartRateSubscription?.cancel();
    _accelSubscription?.cancel();
    _phoneSessionListener?.cancel();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final formattedTime = '${_durationSeconds ~/ 60}:${(_durationSeconds % 60).toString().padLeft(2, '0')}';

    return Scaffold(
      body: Center(
        child: _isRunning
            ? SingleChildScrollView(
                child: Column(
                  mainAxisAlignment: MainAxisAlignment.center,
                  children: [
                    Text(_selectedSport.toUpperCase(),
                        style: const TextStyle(color: Colors.greenAccent, fontSize: 10, letterSpacing: 2)),
                    const SizedBox(height: 2),
                    Text(formattedTime,
                        style: const TextStyle(color: Colors.white, fontSize: 12, fontWeight: FontWeight.bold)),
                    const SizedBox(height: 5),

                    Icon(Icons.favorite, color: _isPaused ? Colors.grey : Colors.redAccent, size: 24),
                    Text('$_displayHr',
                        style: TextStyle(fontSize: 32, fontWeight: FontWeight.bold,
                            color: _isPaused ? Colors.grey : Colors.white)),
                    const Text('BPM', style: TextStyle(fontSize: 10, color: Colors.grey)),

                    if (_selectedSport == 'Alergare') ...[
                      const SizedBox(height: 8),
                      Icon(Icons.directions_run, color: _isPaused ? Colors.grey : Colors.blueAccent, size: 20),
                      Text('$_displaySteps',
                          style: TextStyle(fontSize: 24, fontWeight: FontWeight.bold,
                              color: _isPaused ? Colors.grey : Colors.white)),
                    ] else ...[
                      const SizedBox(height: 10),
                      const Text('Monitorizare\nefort cardiac',
                          textAlign: TextAlign.center, style: TextStyle(fontSize: 10, color: Colors.grey)),
                    ],
                    const SizedBox(height: 12),

                    _isPaused
                        ? Column(children: [
                            const Text('ÎN PAUZĂ',
                                style: TextStyle(color: Colors.orangeAccent, fontSize: 10,
                                    fontWeight: FontWeight.bold, letterSpacing: 1)),
                            const SizedBox(height: 8),
                            Row(mainAxisAlignment: MainAxisAlignment.spaceEvenly, children: [
                              GestureDetector(
                                onTap: _resumeWorkout,
                                child: Container(
                                  padding: const EdgeInsets.all(10),
                                  decoration: BoxDecoration(
                                      shape: BoxShape.circle,
                                      color: Colors.greenAccent.withValues(alpha: 0.2),
                                      border: Border.all(color: Colors.greenAccent)),
                                  child: const Icon(Icons.play_arrow, size: 20, color: Colors.greenAccent),
                                ),
                              ),
                              GestureDetector(
                                onLongPress: _stopWorkout,
                                child: Container(
                                  padding: const EdgeInsets.all(10),
                                  decoration: BoxDecoration(
                                      shape: BoxShape.circle,
                                      color: Colors.redAccent.withValues(alpha: 0.2),
                                      border: Border.all(color: Colors.redAccent)),
                                  child: const Icon(Icons.stop, size: 20, color: Colors.redAccent),
                                ),
                              ),
                            ]),
                            const SizedBox(height: 4),
                            const Row(
                              mainAxisAlignment: MainAxisAlignment.spaceEvenly,
                              children: [
                                Text('apasă', style: TextStyle(fontSize: 8, color: Colors.grey)),
                                Text('ține apăsat', style: TextStyle(fontSize: 8, color: Colors.grey)),
                              ],
                            ),
                          ])
                        : Column(children: [
                            Row(mainAxisAlignment: MainAxisAlignment.spaceEvenly, children: [
                              GestureDetector(
                                onTap: _pauseWorkout,
                                child: Container(
                                  padding: const EdgeInsets.all(10),
                                  decoration: BoxDecoration(
                                      shape: BoxShape.circle, border: Border.all(color: Colors.orangeAccent)),
                                  child: const Icon(Icons.pause, size: 20, color: Colors.orangeAccent),
                                ),
                              ),
                              GestureDetector(
                                onLongPress: _stopWorkout,
                                child: Container(
                                  padding: const EdgeInsets.all(10),
                                  decoration: BoxDecoration(
                                      shape: BoxShape.circle, border: Border.all(color: Colors.redAccent)),
                                  child: const Icon(Icons.stop, size: 20, color: Colors.redAccent),
                                ),
                              ),
                            ]),
                            const SizedBox(height: 4),
                            const Row(
                              mainAxisAlignment: MainAxisAlignment.spaceEvenly,
                              children: [
                                Text('pauză', style: TextStyle(fontSize: 8, color: Colors.grey)),
                                Text('oprire lungă', style: TextStyle(fontSize: 8, color: Colors.grey)),
                              ],
                            ),
                          ]),
                  ],
                ),
              )
            : SingleChildScrollView(
                child: Column(
                  mainAxisAlignment: MainAxisAlignment.center,
                  children: [
                    const Icon(Icons.watch, color: Colors.greenAccent, size: 30),
                    const SizedBox(height: 8),
                    Text(
                      FirebaseAuth.instance.currentUser?.email?.split('@')[0] ?? '',
                      style: const TextStyle(color: Colors.grey, fontSize: 9),
                    ),
                    const SizedBox(height: 10),
                    Container(
                      padding: const EdgeInsets.symmetric(horizontal: 10),
                      decoration: BoxDecoration(color: Colors.grey[900], borderRadius: BorderRadius.circular(15)),
                      child: DropdownButton<String>(
                        value: _selectedSport,
                        dropdownColor: Colors.grey[900],
                        underline: const SizedBox(),
                        style: const TextStyle(color: Colors.white, fontSize: 14),
                        items: _sports.map((s) => DropdownMenuItem(value: s, child: Text(s))).toList(),
                        onChanged: (v) => setState(() => _selectedSport = v!),
                      ),
                    ),
                    const SizedBox(height: 15),
                    ElevatedButton(
                      style: ElevatedButton.styleFrom(
                          backgroundColor: Colors.greenAccent, foregroundColor: Colors.black),
                      onPressed: _startWorkout,
                      child: const Icon(Icons.play_arrow),
                    ),
                    const SizedBox(height: 10),
                    TextButton(
                      onPressed: () async {
                        await FirebaseAuth.instance.signOut();
                        if (context.mounted) {
                          Navigator.pushReplacement(context,
                              MaterialPageRoute(builder: (_) => const WatchLoginScreen()));
                        }
                      },
                      child: const Text('Deconectare', style: TextStyle(color: Colors.grey, fontSize: 9)),
                    ),
                  ],
                ),
              ),
      ),
    );
  }
}

// ==========================================
// DIALOG CĂZĂTURĂ — identic cu ce aveai
// ==========================================
class WatchFallDialog extends StatefulWidget {
  const WatchFallDialog({super.key});
  @override
  State<WatchFallDialog> createState() => _WatchFallDialogState();
}

class _WatchFallDialogState extends State<WatchFallDialog> {
  int _timer = 15;
  late Timer _t;

  @override
  void initState() {
    super.initState();
    _t = Timer.periodic(const Duration(seconds: 1), (t) {
      if (_timer > 0) setState(() => _timer--);
      else { t.cancel(); Navigator.pop(context, true); }
    });
  }

  @override
  void dispose() { _t.cancel(); super.dispose(); }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: Colors.black.withValues(alpha: 0.9),
      body: Center(
        child: Column(
          mainAxisAlignment: MainAxisAlignment.center,
          children: [
            const Icon(Icons.warning_amber, color: Colors.redAccent, size: 30),
            Text('AJUTOR? ($_timer)', style: const TextStyle(fontSize: 14, fontWeight: FontWeight.bold)),
            const SizedBox(height: 10),
            Row(
              mainAxisAlignment: MainAxisAlignment.spaceEvenly,
              children: [
                IconButton(
                  icon: const Icon(Icons.check_circle, color: Colors.greenAccent, size: 35),
                  onPressed: () => Navigator.pop(context, false),
                ),
                IconButton(
                  icon: const Icon(Icons.phone, color: Colors.redAccent, size: 35),
                  onPressed: () => Navigator.pop(context, true),
                ),
              ],
            ),
          ],
        ),
      ),
    );
  }
}

// ==========================================
// SUMAR ANTRENAMENT CEAS — cu toate datele noi
// ==========================================
class WatchSummaryScreen extends StatefulWidget {
  final String sport;
  final int steps;
  final int avgHr;
  final int chronoAge;
  final int durationSeconds;

  const WatchSummaryScreen({
    super.key,
    required this.sport,
    required this.steps,
    required this.avgHr,
    required this.chronoAge,
    required this.durationSeconds,
  });

  @override
  State<WatchSummaryScreen> createState() => _WatchSummaryScreenState();
}

class _WatchSummaryScreenState extends State<WatchSummaryScreen> {
  bool _isSaving = false;
  bool _isSaved = false;

  late double _calculatedBioAge;
  late double _distanceKm;
  late int _caloriesBurned;
  late List<int> _mockHrHistory;

  @override
  void initState() {
    super.initState();
    _calculateMetrics();
  }

  void _calculateMetrics() {
    double age = widget.chronoAge.toDouble();
    if (widget.avgHr > 150) { age += 3.5; }
    else if (widget.avgHr >= 110 && widget.avgHr <= 150) { age -= 2.0; }
    else if (widget.avgHr < 60 && widget.avgHr > 30) { age -= 1.0; }
    else { age += 1.5; }

    if (widget.sport == 'Alergare' && widget.steps > 500) age -= 1.5;
    else if (widget.steps < 50) age += 1.0;

    final variance = (Random().nextDouble() * 1.6) - 0.8;
    _calculatedBioAge = age + variance;

    // Distanța din pași
    _distanceKm = (widget.steps * 0.762) / 1000.0;

    // Calorii (formula Kaggle)
    const double weight = 70.0;
    final double timeInMin = widget.durationSeconds / 60.0;
    final double kcal = ((widget.chronoAge * 0.2017) + (weight * 0.1988) +
        (widget.avgHr * 0.6309) - 55.0969) * timeInMin / 4.184;
    _caloriesBurned = kcal < 0 ? 0 : kcal.round();

    _mockHrHistory = [];
    final baseHr = widget.avgHr > 0 ? widget.avgHr : 80;
    for (int i = 0; i < 5; i++) {
      _mockHrHistory.add(baseHr + (Random().nextInt(24) - 12));
    }
  }

  double get _progressValue => (((widget.chronoAge - _calculatedBioAge) + 5) / 10).clamp(0.1, 1.0);
  Color get _progressColor => _calculatedBioAge <= widget.chronoAge ? Colors.greenAccent : Colors.orangeAccent;

  Future<void> _saveToFirestore() async {
    final uid = FirebaseAuth.instance.currentUser?.uid;
    if (uid == null) return;

    setState(() => _isSaving = true);
    try {
      await FirebaseFirestore.instance
          .collection('users')
          .doc(uid)
          .collection('workouts') // Aceeași colecție ca telefonul
          .add({
        'sportName': widget.sport,
        'avgHr': widget.avgHr,
        'bioAgeResult': _calculatedBioAge.round(),
        'steps': widget.steps,
        'distanceKm': double.parse(_distanceKm.toStringAsFixed(2)),
        'caloriesBurned': _caloriesBurned,
        'durationSeconds': widget.durationSeconds,
        'date': Timestamp.now(), // Data + ora automat
        'source': 'watch',
      });
      if (mounted) setState(() { _isSaving = false; _isSaved = true; });
    } catch (_) {
      if (mounted) setState(() => _isSaving = false);
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: Colors.black,
      body: ListView(
        physics: const AlwaysScrollableScrollPhysics(),
        padding: const EdgeInsets.only(top: 40, bottom: 60, left: 10, right: 10),
        children: [
          const SizedBox(height: 10),
          Text(widget.sport.toUpperCase(),
              textAlign: TextAlign.center,
              style: const TextStyle(color: Colors.grey, fontSize: 10, letterSpacing: 2)),
          const SizedBox(height: 15),

          // Cerc animat Bio-Age — identic cu ce aveai
          Center(
            child: SizedBox(
              width: 110, height: 110,
              child: TweenAnimationBuilder<double>(
                tween: Tween<double>(begin: 0.0, end: _progressValue),
                duration: const Duration(seconds: 2),
                curve: Curves.easeOutCubic,
                builder: (_, value, __) => Stack(
                  fit: StackFit.expand,
                  children: [
                    CircularProgressIndicator(
                        value: 1.0, strokeWidth: 8,
                        color: Colors.grey.withValues(alpha: 0.2)),
                    CircularProgressIndicator(
                        value: value, strokeWidth: 8,
                        backgroundColor: Colors.transparent,
                        valueColor: AlwaysStoppedAnimation<Color>(_progressColor)),
                    Column(mainAxisAlignment: MainAxisAlignment.center, children: [
                      const Text('BIO-AGE', style: TextStyle(fontSize: 10, color: Colors.grey)),
                      Text(_calculatedBioAge.toStringAsFixed(1),
                          style: TextStyle(fontSize: 26, fontWeight: FontWeight.bold,
                              color: _progressColor, height: 1.1)),
                      Text('ani', style: TextStyle(fontSize: 10, color: _progressColor)),
                    ]),
                  ],
                ),
              ),
            ),
          ),
          const SizedBox(height: 15),
          const Icon(Icons.keyboard_arrow_down, color: Colors.white54, size: 20),
          const SizedBox(height: 10),
          const Divider(color: Colors.white24, height: 1),
          const SizedBox(height: 15),

          // NOU: Statistici complete
          Row(mainAxisAlignment: MainAxisAlignment.spaceEvenly, children: [
            _buildSmallStat(Icons.route, Colors.orangeAccent, '${_distanceKm.toStringAsFixed(2)} km', 'DISTANȚĂ'),
            _buildSmallStat(Icons.local_fire_department, Colors.redAccent, '$_caloriesBurned', 'KCAL ARSE'),
          ]),
          const SizedBox(height: 10),
          Row(mainAxisAlignment: MainAxisAlignment.spaceEvenly, children: [
            _buildSmallStat(Icons.directions_run, Colors.blueAccent, '${widget.steps}', 'PAȘI'),
            _buildSmallStat(Icons.timer, Colors.purpleAccent,
                '${widget.durationSeconds ~/ 60}m ${widget.durationSeconds % 60}s', 'DURATĂ'),
          ]),

          const SizedBox(height: 15),
          const Divider(color: Colors.white24, height: 1),
          const SizedBox(height: 15),

          // Grafic puls — identic cu ce aveai
          const Text('EVOLUȚIE PULS', textAlign: TextAlign.center,
              style: TextStyle(fontSize: 8, color: Colors.white70, letterSpacing: 1)),
          const SizedBox(height: 10),
          SizedBox(
            height: 45,
            child: Row(
              mainAxisAlignment: MainAxisAlignment.spaceEvenly,
              crossAxisAlignment: CrossAxisAlignment.end,
              children: _mockHrHistory.map((hr) => _buildGraphBar(hr)).toList(),
            ),
          ),

          const SizedBox(height: 15),
          const Divider(color: Colors.white24, height: 1),
          const SizedBox(height: 15),

          Row(mainAxisAlignment: MainAxisAlignment.spaceEvenly, children: [
            _buildSmallStat(Icons.favorite, Colors.pinkAccent, '${widget.avgHr}', 'BPM MEDIU'),
            _buildSmallStat(Icons.monitor_heart, _progressColor,
                _calculatedBioAge.toStringAsFixed(1), 'BIO-AGE'),
          ]),
          const SizedBox(height: 25),

          // Buton salvare
          _isSaved
              ? Column(children: [
                  const Icon(Icons.check_circle, color: Colors.greenAccent, size: 24),
                  const SizedBox(height: 4),
                  const Text('Salvat în Cloud!', style: TextStyle(color: Colors.greenAccent, fontSize: 10)),
                ])
              : _isSaving
                  ? const Center(child: CircularProgressIndicator(color: Colors.greenAccent, strokeWidth: 2))
                  : Center(
                      child: ElevatedButton.icon(
                        style: ElevatedButton.styleFrom(
                          backgroundColor: Colors.greenAccent, foregroundColor: Colors.black,
                          minimumSize: const Size(120, 35),
                        ),
                        onPressed: _saveToFirestore,
                        icon: const Icon(Icons.cloud_upload, size: 14),
                        label: const Text('SALVEAZĂ', style: TextStyle(fontSize: 11, fontWeight: FontWeight.bold)),
                      ),
                    ),

          const SizedBox(height: 15),
          Center(
            child: IconButton(
              icon: const Icon(Icons.close, color: Colors.white54, size: 30),
              onPressed: () => Navigator.pushReplacement(
                  context, MaterialPageRoute(builder: (_) => const WatchSensorScreen())),
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildGraphBar(int hr) {
    final barHeight = (hr / 200.0) * 30.0;
    final barColor = hr > 140 ? Colors.redAccent : (hr > 100 ? Colors.orangeAccent : Colors.greenAccent);
    return Column(
      mainAxisAlignment: MainAxisAlignment.end,
      children: [
        Text('$hr', style: const TextStyle(fontSize: 7, color: Colors.white70)),
        const SizedBox(height: 2),
        Container(
          width: 14, height: barHeight,
          decoration: BoxDecoration(
              color: barColor, borderRadius: const BorderRadius.vertical(top: Radius.circular(3))),
        ),
      ],
    );
  }

  Widget _buildSmallStat(IconData icon, Color color, String value, String label) {
    return Column(children: [
      Icon(icon, color: color, size: 18),
      const SizedBox(height: 4),
      Text(value, style: const TextStyle(fontSize: 16, fontWeight: FontWeight.bold, color: Colors.white)),
      Text(label, style: const TextStyle(fontSize: 8, color: Colors.grey)),
    ]);
  }
}