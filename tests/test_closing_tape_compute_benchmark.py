from backend.closing_tape.compute_benchmark import benchmark_tabular_training


def test_cpu_compute_benchmark_is_a_throughput_measurement_not_accuracy_claim():
    report = benchmark_tabular_training(
        samples=256,
        input_features=8,
        batch_size=64,
        iterations=2,
        warmup_iterations=1,
        devices=("cpu",),
    )

    assert report.results[0].device == "cpu"
    assert report.results[0].samples == 128
    assert report.results[0].samples_per_second > 0
    assert report.recommended_training_device == "cpu"
    assert report.accuracy_claim is False
