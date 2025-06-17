# World Quant Brain API

A simple wrapper of world quant api.

Examples:

- Search fields

```python
cli = brain.Client("user", "pass")

fields = cli.data_fields().with_filter(data_type="MATRIX", dataset_id="fundamental6").limit(70)

for item in fields.iter()
    print(f"id: {item.id}, description: {item.description}")
```

- Submit simulation

```python
cli = brain.Client("user", "pass")

sim = cli.simulation()
result = sim.with_expr("returns").send().wait()
print(result.alpha)

alpha_detail = result.detail()
print(alpha_detail)
```
