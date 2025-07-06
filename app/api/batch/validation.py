from fastapi import HTTPException
from app.validation import validate_device_data

def validate_batch_structure(data, batch_type="batch"):
    if not isinstance(data, list):
        raise HTTPException(status_code=400, detail=f"Expected JSON array for {batch_type}")
    
    if len(data) == 0:
        raise HTTPException(status_code=400, detail=f"Empty {batch_type} not allowed")
    
    if len(data) > 5000:
        raise HTTPException(status_code=413, detail=f"Batch too large: {len(data)} items (max 5000)")
    
    errors = []
    warnings = []
    valid_items = 0
    
    for i, item in enumerate(data):
        if not isinstance(item, dict):
            errors.append(f"Item {i}: Expected object, got {type(item).__name__}")
            continue
        
        validation = validate_device_data(item)
        if not validation['is_valid']:
            errors.append(f"Item {i}: {validation['errors']}")
        else:
            valid_items += 1
        
        if validation.get('warnings'):
            warnings.extend([f"Item {i}: {w}" for w in validation['warnings']])
    
    error_rate = len(errors) / len(data)
    if error_rate > 0.5:
        raise HTTPException(status_code=400, detail=f"Too many invalid items: {len(errors)}/{len(data)} failed validation")
    
    return {
        'total_items': len(data),
        'valid_items': valid_items,
        'error_count': len(errors),
        'warning_count': len(warnings),
        'errors': errors[:10],
        'warnings': warnings[:20]
    }
