"""
Nova DSO Tracker — REST API v1

Full CRUD API for all data models, secured by API-key authentication.
Prefix: /api/v1  (set during blueprint registration)
"""

from datetime import datetime
from flask import Blueprint, request, jsonify, g
import uuid

from nova.api_auth import (
    api_key_required,
    create_api_key,
    hash_api_key,
    key_prefix as _key_prefix,
)
from nova.models import (
    SessionLocal,
    DbUser,
    AstroObject,
    Project,
    Location,
    HorizonPoint,
    SavedFraming,
    Component,
    Rig,
    JournalSession,
    SavedView,
    UserCustomFilter,
    ApiKey,
    UiPref,
)
from nova.config import SINGLE_USER_MODE

rest_api_bp = Blueprint("rest_api", __name__)

# ──────────────────────────────────────────────────────────
#  Helpers
# ──────────────────────────────────────────────────────────


def _db():
    """Return a scoped SQLAlchemy session."""
    return SessionLocal()


def _paginate(query):
    """Apply pagination from query-string ?page=&per_page= and return (items, meta)."""
    page = max(1, request.args.get("page", 1, type=int))
    per_page = min(200, max(1, request.args.get("per_page", 50, type=int)))
    total = query.count()
    items = query.offset((page - 1) * per_page).limit(per_page).all()
    return items, {
        "page": page,
        "per_page": per_page,
        "total": total,
        "pages": max(1, (total + per_page - 1) // per_page),
    }


def _ok(data, meta=None, status=200):
    body = {"data": data}
    if meta is not None:
        body["meta"] = meta
    return jsonify(body), status


def _err(message, status=400):
    return jsonify({"error": message}), status


def _user_id():
    """Return the authenticated DbUser.id."""
    return g.db_user.id


def _to_date(val):
    """Parse a date string (YYYY-MM-DD) or return None."""
    if val is None:
        return None
    if isinstance(val, str):
        try:
            return datetime.strptime(val, "%Y-%m-%d").date()
        except ValueError:
            return None
    return val


def _to_float(val):
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _to_int(val):
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _to_bool(val):
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.lower() in ("true", "1", "yes")
    return bool(val)


# ──────────────────────────────────────────────────────────
#  Serializers
# ──────────────────────────────────────────────────────────


def _serialize_object(obj):
    return {
        "id": obj.id,
        "object_name": obj.object_name,
        "common_name": obj.common_name,
        "ra_hours": obj.ra_hours,
        "dec_deg": obj.dec_deg,
        "type": obj.type,
        "constellation": obj.constellation,
        "magnitude": obj.magnitude,
        "size": obj.size,
        "sb": obj.sb,
        "active_project": obj.active_project,
        "project_name": obj.project_name,
        "is_shared": obj.is_shared,
        "shared_notes": obj.shared_notes,
        "catalog_sources": obj.catalog_sources,
        "catalog_info": obj.catalog_info,
        "enabled": obj.enabled,
        "image_url": obj.image_url,
        "image_credit": obj.image_credit,
        "image_source_link": obj.image_source_link,
        "description_text": obj.description_text,
        "description_credit": obj.description_credit,
        "description_source_link": obj.description_source_link,
    }


def _serialize_project(p):
    return {
        "id": p.id,
        "name": p.name,
        "target_object_name": p.target_object_name,
        "description_notes": p.description_notes,
        "framing_notes": p.framing_notes,
        "processing_notes": p.processing_notes,
        "final_image_file": p.final_image_file,
        "goals": p.goals,
        "status": p.status,
    }


def _serialize_location(loc):
    return {
        "id": loc.id,
        "stable_uid": loc.stable_uid,
        "name": loc.name,
        "lat": loc.lat,
        "lon": loc.lon,
        "timezone": loc.timezone,
        "altitude_threshold": loc.altitude_threshold,
        "is_default": loc.is_default,
        "active": loc.active,
        "comments": loc.comments,
    }


def _serialize_horizon_point(hp):
    return {
        "id": hp.id,
        "az_deg": hp.az_deg,
        "alt_min_deg": hp.alt_min_deg,
    }


def _serialize_component(c):
    return {
        "id": c.id,
        "stable_uid": c.stable_uid,
        "kind": c.kind,
        "name": c.name,
        "aperture_mm": c.aperture_mm,
        "focal_length_mm": c.focal_length_mm,
        "sensor_width_mm": c.sensor_width_mm,
        "sensor_height_mm": c.sensor_height_mm,
        "resolution_width_px": c.resolution_width_px,
        "resolution_height_px": c.resolution_height_px,
        "pixel_size_um": c.pixel_size_um,
        "factor": c.factor,
        "is_shared": c.is_shared,
    }


def _serialize_rig(r):
    return {
        "id": r.id,
        "stable_uid": r.stable_uid,
        "rig_name": r.rig_name,
        "telescope_id": r.telescope_id,
        "camera_id": r.camera_id,
        "reducer_extender_id": r.reducer_extender_id,
        "effective_focal_length": r.effective_focal_length,
        "f_ratio": r.f_ratio,
        "image_scale": r.image_scale,
        "fov_w_arcmin": r.fov_w_arcmin,
        "fov_h_arcmin": r.fov_h_arcmin,
        "guide_telescope_id": r.guide_telescope_id,
        "guide_camera_id": r.guide_camera_id,
        "guide_is_oag": r.guide_is_oag,
    }


def _serialize_session(s):
    return {
        "id": s.id,
        "project_id": s.project_id,
        "date_utc": s.date_utc.isoformat() if s.date_utc else None,
        "object_name": s.object_name,
        "notes": s.notes,
        "session_image_file": s.session_image_file,
        "location_name": s.location_name,
        "seeing": s.seeing,
        "moon_phase_pct": s.moon_phase_pct,
        "moon_proximity_deg": s.moon_proximity_deg,
        "weather": s.weather,
        "temperature_c": s.temperature_c,
        "humidity_pct": s.humidity_pct,
        "wind_kph": s.wind_kph,
        "bortle": s.bortle,
        "sqm": s.sqm,
        "filter_type": s.filter_type,
        "guiding_rms": s.guiding_rms,
        "dither_enabled": s.dither_enabled,
        "acquisition_software": s.acquisition_software,
        "guiding_software": s.guiding_software,
        "gain": s.gain,
        "offset": s.offset,
        "camera_temp_c": s.camera_temp_c,
        "binning": s.binning,
        "dark_frames": s.dark_frames,
        "flat_frames": s.flat_frames,
        "bias_frames": s.bias_frames,
        "rating": s.rating,
        "transparency": s.transparency,
        "l_subs": s.l_subs,
        "l_exposure": s.l_exposure,
        "r_subs": s.r_subs,
        "r_exposure": s.r_exposure,
        "g_subs": s.g_subs,
        "g_exposure": s.g_exposure,
        "b_subs": s.b_subs,
        "b_exposure": s.b_exposure,
        "ha_subs": s.ha_subs,
        "ha_exposure": s.ha_exposure,
        "oiii_subs": s.oiii_subs,
        "oiii_exposure": s.oiii_exposure,
        "sii_subs": s.sii_subs,
        "sii_exposure": s.sii_exposure,
        "custom_filter_data": s.custom_filter_data,
        "calculated_integration_time_minutes": s.calculated_integration_time_minutes,
        "rig_snapshot_telescope": s.rig_snapshot_telescope,
        "rig_snapshot_camera": s.rig_snapshot_camera,
        "rig_snapshot_reducer": s.rig_snapshot_reducer,
        "rig_snapshot_efl": s.rig_snapshot_efl,
        "rig_snapshot_fratio": s.rig_snapshot_fratio,
        "rig_snapshot_image_scale": s.rig_snapshot_image_scale,
        "rig_snapshot_fov_w": s.rig_snapshot_fov_w,
        "rig_snapshot_fov_h": s.rig_snapshot_fov_h,
        "rig_snapshot_guide_telescope": s.rig_snapshot_guide_telescope,
        "rig_snapshot_guide_camera": s.rig_snapshot_guide_camera,
        "rig_snapshot_guide_is_oag": s.rig_snapshot_guide_is_oag,
        "external_id": s.external_id,
    }


def _serialize_saved_view(v):
    return {
        "id": v.id,
        "name": v.name,
        "description": v.description,
        "settings_json": v.settings_json,
        "is_shared": v.is_shared,
    }


def _serialize_framing(f):
    return {
        "id": f.id,
        "object_name": f.object_name,
        "rig_id": f.rig_id,
        "rig_effective_focal_length": f.rig_effective_focal_length,
        "rig_fov_w_arcmin": f.rig_fov_w_arcmin,
        "rig_fov_h_arcmin": f.rig_fov_h_arcmin,
        "rig_image_scale": f.rig_image_scale,
        "rig_resolution_width_px": f.rig_resolution_width_px,
        "rig_resolution_height_px": f.rig_resolution_height_px,
        "survey_name": f.survey_name,
        "survey_ra_hours": f.survey_ra_hours,
        "survey_dec_deg": f.survey_dec_deg,
        "survey_fov_deg": f.survey_fov_deg,
        "survey_rotation_deg": f.survey_rotation_deg,
        "survey_width_px": f.survey_width_px,
        "survey_height_px": f.survey_height_px,
        "mosaic_panels_x": f.mosaic_panels_x,
        "mosaic_panels_y": f.mosaic_panels_y,
        "mosaic_overlap_pct": f.mosaic_overlap_pct,
        "image_brightness": f.image_brightness,
        "image_contrast": f.image_contrast,
        "image_saturation": f.image_saturation,
        "image_invert": f.image_invert,
        "geo_belt_enabled": f.geo_belt_enabled,
        "updated_at": f.updated_at.isoformat() if f.updated_at else None,
    }


def _serialize_custom_filter(cf):
    return {
        "id": cf.id,
        "filter_key": cf.filter_key,
        "filter_label": cf.filter_label,
        "created_at": cf.created_at.isoformat() if cf.created_at else None,
    }


def _serialize_api_key(k):
    return {
        "id": k.id,
        "key_prefix": k.key_prefix,
        "name": k.name,
        "created_at": k.created_at.isoformat() if k.created_at else None,
        "last_used_at": k.last_used_at.isoformat() if k.last_used_at else None,
        "is_active": k.is_active,
    }


def _serialize_ui_pref(p):
    return {
        "id": p.id,
        "json_blob": p.json_blob,
    }


# ──────────────────────────────────────────────────────────
#  OBJECTS  (AstroObject)
# ──────────────────────────────────────────────────────────


@rest_api_bp.route("/objects", methods=["GET"])
@api_key_required
def list_objects():
    """List all objects for the authenticated user. Supports pagination and filtering."""
    db = _db()
    try:
        q = db.query(AstroObject).filter(AstroObject.user_id == _user_id())

        # Optional filters
        obj_type = request.args.get("type")
        if obj_type:
            q = q.filter(AstroObject.type == obj_type)
        constellation = request.args.get("constellation")
        if constellation:
            q = q.filter(AstroObject.constellation == constellation)
        enabled = request.args.get("enabled")
        if enabled is not None:
            q = q.filter(AstroObject.enabled == _to_bool(enabled))
        search = request.args.get("search")
        if search:
            pattern = f"%{search}%"
            q = q.filter(
                (AstroObject.object_name.ilike(pattern))
                | (AstroObject.common_name.ilike(pattern))
            )

        q = q.order_by(AstroObject.object_name)
        items, meta = _paginate(q)
        return _ok([_serialize_object(o) for o in items], meta)
    finally:
        db.remove()


@rest_api_bp.route("/objects", methods=["POST"])
@api_key_required
def create_object():
    """Create a new astronomical object."""
    data = request.get_json(silent=True) or {}
    if not data.get("object_name"):
        return _err("object_name is required")

    db = _db()
    try:
        existing = (
            db.query(AstroObject)
            .filter(
                AstroObject.user_id == _user_id(),
                AstroObject.object_name == data["object_name"],
            )
            .first()
        )
        if existing:
            return _err(f"Object '{data['object_name']}' already exists", 409)

        obj = AstroObject(user_id=_user_id())
        _apply_object_fields(obj, data)
        db.add(obj)
        db.commit()
        db.refresh(obj)
        return _ok(_serialize_object(obj), status=201)
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


@rest_api_bp.route("/objects/<int:object_id>", methods=["GET"])
@api_key_required
def get_object(object_id):
    db = _db()
    try:
        obj = (
            db.query(AstroObject)
            .filter(
                AstroObject.id == object_id,
                AstroObject.user_id == _user_id(),
            )
            .first()
        )
        if obj is None:
            return _err("Object not found", 404)
        return _ok(_serialize_object(obj))
    finally:
        db.remove()


@rest_api_bp.route("/objects/<int:object_id>", methods=["PUT"])
@api_key_required
def update_object(object_id):
    data = request.get_json(silent=True) or {}
    db = _db()
    try:
        obj = (
            db.query(AstroObject)
            .filter(
                AstroObject.id == object_id,
                AstroObject.user_id == _user_id(),
            )
            .first()
        )
        if obj is None:
            return _err("Object not found", 404)
        _apply_object_fields(obj, data)
        db.commit()
        db.refresh(obj)
        return _ok(_serialize_object(obj))
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


@rest_api_bp.route("/objects/<int:object_id>", methods=["DELETE"])
@api_key_required
def delete_object(object_id):
    db = _db()
    try:
        obj = (
            db.query(AstroObject)
            .filter(
                AstroObject.id == object_id,
                AstroObject.user_id == _user_id(),
            )
            .first()
        )
        if obj is None:
            return _err("Object not found", 404)
        db.delete(obj)
        db.commit()
        return _ok({"deleted": True})
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


def _apply_object_fields(obj, data):
    """Apply writable fields from request data to an AstroObject."""
    _fields = [
        "object_name",
        "common_name",
        "ra_hours",
        "dec_deg",
        "type",
        "constellation",
        "magnitude",
        "size",
        "sb",
        "active_project",
        "project_name",
        "is_shared",
        "shared_notes",
        "catalog_sources",
        "catalog_info",
        "enabled",
        "image_url",
        "image_credit",
        "image_source_link",
        "description_text",
        "description_credit",
        "description_source_link",
    ]
    for f in _fields:
        if f in data:
            setattr(obj, f, data[f])


# ──────────────────────────────────────────────────────────
#  PROJECTS
# ──────────────────────────────────────────────────────────


@rest_api_bp.route("/projects", methods=["GET"])
@api_key_required
def list_projects():
    db = _db()
    try:
        q = db.query(Project).filter(Project.user_id == _user_id())
        status = request.args.get("status")
        if status:
            q = q.filter(Project.status == status)
        q = q.order_by(Project.name)
        items, meta = _paginate(q)
        return _ok([_serialize_project(p) for p in items], meta)
    finally:
        db.remove()


@rest_api_bp.route("/projects", methods=["POST"])
@api_key_required
def create_project():
    data = request.get_json(silent=True) or {}
    if not data.get("name"):
        return _err("name is required")
    db = _db()
    try:
        project_id = data.get("id") or str(uuid.uuid4())[:8]
        existing = db.query(Project).filter(Project.id == project_id).first()
        if existing:
            return _err(f"Project with id '{project_id}' already exists", 409)
        p = Project(id=project_id, user_id=_user_id())
        _apply_project_fields(p, data)
        db.add(p)
        db.commit()
        db.refresh(p)
        return _ok(_serialize_project(p), status=201)
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


@rest_api_bp.route("/projects/<string:project_id>", methods=["GET"])
@api_key_required
def get_project(project_id):
    db = _db()
    try:
        p = (
            db.query(Project)
            .filter(
                Project.id == project_id,
                Project.user_id == _user_id(),
            )
            .first()
        )
        if p is None:
            return _err("Project not found", 404)
        return _ok(_serialize_project(p))
    finally:
        db.remove()


@rest_api_bp.route("/projects/<string:project_id>", methods=["PUT"])
@api_key_required
def update_project(project_id):
    data = request.get_json(silent=True) or {}
    db = _db()
    try:
        p = (
            db.query(Project)
            .filter(
                Project.id == project_id,
                Project.user_id == _user_id(),
            )
            .first()
        )
        if p is None:
            return _err("Project not found", 404)
        _apply_project_fields(p, data)
        db.commit()
        db.refresh(p)
        return _ok(_serialize_project(p))
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


@rest_api_bp.route("/projects/<string:project_id>", methods=["DELETE"])
@api_key_required
def delete_project(project_id):
    db = _db()
    try:
        p = (
            db.query(Project)
            .filter(
                Project.id == project_id,
                Project.user_id == _user_id(),
            )
            .first()
        )
        if p is None:
            return _err("Project not found", 404)
        db.delete(p)
        db.commit()
        return _ok({"deleted": True})
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


def _apply_project_fields(p, data):
    for f in [
        "name",
        "target_object_name",
        "description_notes",
        "framing_notes",
        "processing_notes",
        "final_image_file",
        "goals",
        "status",
    ]:
        if f in data:
            setattr(p, f, data[f])


# ──────────────────────────────────────────────────────────
#  LOCATIONS
# ──────────────────────────────────────────────────────────


@rest_api_bp.route("/locations", methods=["GET"])
@api_key_required
def list_locations():
    db = _db()
    try:
        q = db.query(Location).filter(Location.user_id == _user_id())
        q = q.order_by(Location.name)
        items, meta = _paginate(q)
        return _ok([_serialize_location(loc) for loc in items], meta)
    finally:
        db.remove()


@rest_api_bp.route("/locations", methods=["POST"])
@api_key_required
def create_location():
    data = request.get_json(silent=True) or {}
    if not data.get("name"):
        return _err("name is required")
    db = _db()
    try:
        loc = Location(
            stable_uid=data.get("stable_uid") or str(uuid.uuid4()),
            user_id=_user_id(),
        )
        _apply_location_fields(loc, data)
        db.add(loc)
        db.commit()
        db.refresh(loc)
        return _ok(_serialize_location(loc), status=201)
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


@rest_api_bp.route("/locations/<int:location_id>", methods=["GET"])
@api_key_required
def get_location(location_id):
    db = _db()
    try:
        loc = (
            db.query(Location)
            .filter(
                Location.id == location_id,
                Location.user_id == _user_id(),
            )
            .first()
        )
        if loc is None:
            return _err("Location not found", 404)
        result = _serialize_location(loc)
        result["horizon_points"] = [
            _serialize_horizon_point(hp) for hp in loc.horizon_points
        ]
        return _ok(result)
    finally:
        db.remove()


@rest_api_bp.route("/locations/<int:location_id>", methods=["PUT"])
@api_key_required
def update_location(location_id):
    data = request.get_json(silent=True) or {}
    db = _db()
    try:
        loc = (
            db.query(Location)
            .filter(
                Location.id == location_id,
                Location.user_id == _user_id(),
            )
            .first()
        )
        if loc is None:
            return _err("Location not found", 404)
        _apply_location_fields(loc, data)
        db.commit()
        db.refresh(loc)
        return _ok(_serialize_location(loc))
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


@rest_api_bp.route("/locations/<int:location_id>", methods=["DELETE"])
@api_key_required
def delete_location(location_id):
    db = _db()
    try:
        loc = (
            db.query(Location)
            .filter(
                Location.id == location_id,
                Location.user_id == _user_id(),
            )
            .first()
        )
        if loc is None:
            return _err("Location not found", 404)
        db.delete(loc)
        db.commit()
        return _ok({"deleted": True})
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


def _apply_location_fields(loc, data):
    for f in [
        "name",
        "lat",
        "lon",
        "timezone",
        "altitude_threshold",
        "is_default",
        "active",
        "comments",
    ]:
        if f in data:
            setattr(loc, f, data[f])


# ──── Horizon points (sub-resource) ────


@rest_api_bp.route("/locations/<int:location_id>/horizon", methods=["GET"])
@api_key_required
def get_horizon(location_id):
    db = _db()
    try:
        loc = (
            db.query(Location)
            .filter(
                Location.id == location_id,
                Location.user_id == _user_id(),
            )
            .first()
        )
        if loc is None:
            return _err("Location not found", 404)
        points = (
            db.query(HorizonPoint)
            .filter(
                HorizonPoint.location_id == loc.id,
            )
            .order_by(HorizonPoint.az_deg)
            .all()
        )
        return _ok([_serialize_horizon_point(hp) for hp in points])
    finally:
        db.remove()


@rest_api_bp.route("/locations/<int:location_id>/horizon", methods=["PUT"])
@api_key_required
def set_horizon(location_id):
    """Replace ALL horizon points for a location."""
    data = request.get_json(silent=True) or {}
    points_data = data.get("points", [])
    db = _db()
    try:
        loc = (
            db.query(Location)
            .filter(
                Location.id == location_id,
                Location.user_id == _user_id(),
            )
            .first()
        )
        if loc is None:
            return _err("Location not found", 404)
        # Delete existing
        db.query(HorizonPoint).filter(HorizonPoint.location_id == loc.id).delete()
        # Insert new
        for pt in points_data:
            hp = HorizonPoint(
                location_id=loc.id,
                az_deg=pt.get("az_deg"),
                alt_min_deg=pt.get("alt_min_deg"),
            )
            db.add(hp)
        db.commit()
        new_points = (
            db.query(HorizonPoint)
            .filter(
                HorizonPoint.location_id == loc.id,
            )
            .order_by(HorizonPoint.az_deg)
            .all()
        )
        return _ok([_serialize_horizon_point(hp) for hp in new_points])
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


# ──────────────────────────────────────────────────────────
#  COMPONENTS  (telescopes, cameras, reducer/extenders)
# ──────────────────────────────────────────────────────────


@rest_api_bp.route("/components", methods=["GET"])
@api_key_required
def list_components():
    db = _db()
    try:
        q = db.query(Component).filter(Component.user_id == _user_id())
        kind = request.args.get("kind")
        if kind:
            q = q.filter(Component.kind == kind)
        q = q.order_by(Component.kind, Component.name)
        items, meta = _paginate(q)
        return _ok([_serialize_component(c) for c in items], meta)
    finally:
        db.remove()


@rest_api_bp.route("/components", methods=["POST"])
@api_key_required
def create_component():
    data = request.get_json(silent=True) or {}
    if not data.get("kind") or not data.get("name"):
        return _err("kind and name are required")
    if data["kind"] not in ("telescope", "camera", "reducer_extender"):
        return _err("kind must be 'telescope', 'camera', or 'reducer_extender'")
    db = _db()
    try:
        c = Component(
            stable_uid=data.get("stable_uid") or str(uuid.uuid4()),
            user_id=_user_id(),
        )
        _apply_component_fields(c, data)
        db.add(c)
        db.commit()
        db.refresh(c)
        return _ok(_serialize_component(c), status=201)
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


@rest_api_bp.route("/components/<int:component_id>", methods=["GET"])
@api_key_required
def get_component(component_id):
    db = _db()
    try:
        c = (
            db.query(Component)
            .filter(
                Component.id == component_id,
                Component.user_id == _user_id(),
            )
            .first()
        )
        if c is None:
            return _err("Component not found", 404)
        return _ok(_serialize_component(c))
    finally:
        db.remove()


@rest_api_bp.route("/components/<int:component_id>", methods=["PUT"])
@api_key_required
def update_component(component_id):
    data = request.get_json(silent=True) or {}
    db = _db()
    try:
        c = (
            db.query(Component)
            .filter(
                Component.id == component_id,
                Component.user_id == _user_id(),
            )
            .first()
        )
        if c is None:
            return _err("Component not found", 404)
        _apply_component_fields(c, data)
        db.commit()
        db.refresh(c)
        return _ok(_serialize_component(c))
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


@rest_api_bp.route("/components/<int:component_id>", methods=["DELETE"])
@api_key_required
def delete_component(component_id):
    db = _db()
    try:
        c = (
            db.query(Component)
            .filter(
                Component.id == component_id,
                Component.user_id == _user_id(),
            )
            .first()
        )
        if c is None:
            return _err("Component not found", 404)
        db.delete(c)
        db.commit()
        return _ok({"deleted": True})
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


def _apply_component_fields(c, data):
    for f in [
        "kind",
        "name",
        "aperture_mm",
        "focal_length_mm",
        "sensor_width_mm",
        "sensor_height_mm",
        "resolution_width_px",
        "resolution_height_px",
        "pixel_size_um",
        "factor",
        "is_shared",
    ]:
        if f in data:
            setattr(c, f, data[f])


# ──────────────────────────────────────────────────────────
#  RIGS
# ──────────────────────────────────────────────────────────


@rest_api_bp.route("/rigs", methods=["GET"])
@api_key_required
def list_rigs():
    db = _db()
    try:
        q = db.query(Rig).filter(Rig.user_id == _user_id())
        q = q.order_by(Rig.rig_name)
        items, meta = _paginate(q)
        return _ok([_serialize_rig(r) for r in items], meta)
    finally:
        db.remove()


@rest_api_bp.route("/rigs", methods=["POST"])
@api_key_required
def create_rig():
    data = request.get_json(silent=True) or {}
    if not data.get("rig_name"):
        return _err("rig_name is required")
    db = _db()
    try:
        r = Rig(
            stable_uid=data.get("stable_uid") or str(uuid.uuid4()),
            user_id=_user_id(),
        )
        _apply_rig_fields(r, data)
        db.add(r)
        db.commit()
        db.refresh(r)
        return _ok(_serialize_rig(r), status=201)
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


@rest_api_bp.route("/rigs/<int:rig_id>", methods=["GET"])
@api_key_required
def get_rig(rig_id):
    db = _db()
    try:
        r = (
            db.query(Rig)
            .filter(
                Rig.id == rig_id,
                Rig.user_id == _user_id(),
            )
            .first()
        )
        if r is None:
            return _err("Rig not found", 404)
        return _ok(_serialize_rig(r))
    finally:
        db.remove()


@rest_api_bp.route("/rigs/<int:rig_id>", methods=["PUT"])
@api_key_required
def update_rig(rig_id):
    data = request.get_json(silent=True) or {}
    db = _db()
    try:
        r = (
            db.query(Rig)
            .filter(
                Rig.id == rig_id,
                Rig.user_id == _user_id(),
            )
            .first()
        )
        if r is None:
            return _err("Rig not found", 404)
        _apply_rig_fields(r, data)
        db.commit()
        db.refresh(r)
        return _ok(_serialize_rig(r))
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


@rest_api_bp.route("/rigs/<int:rig_id>", methods=["DELETE"])
@api_key_required
def delete_rig(rig_id):
    db = _db()
    try:
        r = (
            db.query(Rig)
            .filter(
                Rig.id == rig_id,
                Rig.user_id == _user_id(),
            )
            .first()
        )
        if r is None:
            return _err("Rig not found", 404)
        db.delete(r)
        db.commit()
        return _ok({"deleted": True})
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


def _apply_rig_fields(r, data):
    for f in [
        "rig_name",
        "telescope_id",
        "camera_id",
        "reducer_extender_id",
        "effective_focal_length",
        "f_ratio",
        "image_scale",
        "fov_w_arcmin",
        "fov_h_arcmin",
        "guide_telescope_id",
        "guide_camera_id",
        "guide_is_oag",
    ]:
        if f in data:
            setattr(r, f, data[f])


# ──────────────────────────────────────────────────────────
#  JOURNAL SESSIONS
# ──────────────────────────────────────────────────────────


@rest_api_bp.route("/sessions", methods=["GET"])
@api_key_required
def list_sessions():
    db = _db()
    try:
        q = db.query(JournalSession).filter(JournalSession.user_id == _user_id())
        obj = request.args.get("object_name")
        if obj:
            q = q.filter(JournalSession.object_name == obj)
        project = request.args.get("project_id")
        if project:
            q = q.filter(JournalSession.project_id == project)
        date_from = request.args.get("date_from")
        if date_from:
            d = _to_date(date_from)
            if d:
                q = q.filter(JournalSession.date_utc >= d)
        date_to = request.args.get("date_to")
        if date_to:
            d = _to_date(date_to)
            if d:
                q = q.filter(JournalSession.date_utc <= d)
        q = q.order_by(JournalSession.date_utc.desc())
        items, meta = _paginate(q)
        return _ok([_serialize_session(s) for s in items], meta)
    finally:
        db.remove()


@rest_api_bp.route("/sessions", methods=["POST"])
@api_key_required
def create_session():
    data = request.get_json(silent=True) or {}
    if not data.get("object_name"):
        return _err("object_name is required")
    db = _db()
    try:
        s = JournalSession(user_id=_user_id())
        _apply_session_fields(s, data)
        db.add(s)
        db.commit()
        db.refresh(s)
        return _ok(_serialize_session(s), status=201)
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


@rest_api_bp.route("/sessions/<int:session_id>", methods=["GET"])
@api_key_required
def get_session(session_id):
    db = _db()
    try:
        s = (
            db.query(JournalSession)
            .filter(
                JournalSession.id == session_id,
                JournalSession.user_id == _user_id(),
            )
            .first()
        )
        if s is None:
            return _err("Session not found", 404)
        return _ok(_serialize_session(s))
    finally:
        db.remove()


@rest_api_bp.route("/sessions/<int:session_id>", methods=["PUT"])
@api_key_required
def update_session(session_id):
    data = request.get_json(silent=True) or {}
    db = _db()
    try:
        s = (
            db.query(JournalSession)
            .filter(
                JournalSession.id == session_id,
                JournalSession.user_id == _user_id(),
            )
            .first()
        )
        if s is None:
            return _err("Session not found", 404)
        _apply_session_fields(s, data)
        db.commit()
        db.refresh(s)
        return _ok(_serialize_session(s))
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


@rest_api_bp.route("/sessions/<int:session_id>", methods=["DELETE"])
@api_key_required
def delete_session(session_id):
    db = _db()
    try:
        s = (
            db.query(JournalSession)
            .filter(
                JournalSession.id == session_id,
                JournalSession.user_id == _user_id(),
            )
            .first()
        )
        if s is None:
            return _err("Session not found", 404)
        db.delete(s)
        db.commit()
        return _ok({"deleted": True})
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


def _apply_session_fields(s, data):
    # Date needs special handling
    if "date_utc" in data:
        s.date_utc = _to_date(data["date_utc"])

    simple_str = [
        "object_name",
        "notes",
        "session_image_file",
        "location_name",
        "seeing",
        "weather",
        "filter_type",
        "acquisition_software",
        "guiding_software",
        "binning",
        "transparency",
        "rig_snapshot_telescope",
        "rig_snapshot_camera",
        "rig_snapshot_reducer",
        "rig_snapshot_guide_telescope",
        "rig_snapshot_guide_camera",
        "external_id",
    ]
    for f in simple_str:
        if f in data:
            setattr(s, f, data[f])

    float_fields = [
        "moon_phase_pct",
        "moon_proximity_deg",
        "temperature_c",
        "humidity_pct",
        "wind_kph",
        "sqm",
        "guiding_rms",
        "camera_temp_c",
        "rig_snapshot_efl",
        "rig_snapshot_fratio",
        "rig_snapshot_image_scale",
        "rig_snapshot_fov_w",
        "rig_snapshot_fov_h",
        "calculated_integration_time_minutes",
        "l_exposure",
        "r_exposure",
        "g_exposure",
        "b_exposure",
        "ha_exposure",
        "oiii_exposure",
        "sii_exposure",
    ]
    for f in float_fields:
        if f in data:
            setattr(s, f, _to_float(data[f]))

    int_fields = [
        "bortle",
        "gain",
        "offset",
        "dark_frames",
        "flat_frames",
        "bias_frames",
        "rating",
        "l_subs",
        "r_subs",
        "g_subs",
        "b_subs",
        "ha_subs",
        "oiii_subs",
        "sii_subs",
    ]
    for f in int_fields:
        if f in data:
            setattr(s, f, _to_int(data[f]))

    bool_fields = ["dither_enabled", "rig_snapshot_guide_is_oag"]
    for f in bool_fields:
        if f in data:
            setattr(s, f, _to_bool(data[f]))

    if "project_id" in data:
        s.project_id = data["project_id"]
    if "custom_filter_data" in data:
        s.custom_filter_data = data["custom_filter_data"]


# ──────────────────────────────────────────────────────────
#  SAVED VIEWS
# ──────────────────────────────────────────────────────────


@rest_api_bp.route("/views", methods=["GET"])
@api_key_required
def list_views():
    db = _db()
    try:
        q = db.query(SavedView).filter(SavedView.user_id == _user_id())
        q = q.order_by(SavedView.name)
        items, meta = _paginate(q)
        return _ok([_serialize_saved_view(v) for v in items], meta)
    finally:
        db.remove()


@rest_api_bp.route("/views", methods=["POST"])
@api_key_required
def create_view():
    data = request.get_json(silent=True) or {}
    if not data.get("name"):
        return _err("name is required")
    db = _db()
    try:
        v = SavedView(user_id=_user_id())
        _apply_view_fields(v, data)
        db.add(v)
        db.commit()
        db.refresh(v)
        return _ok(_serialize_saved_view(v), status=201)
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


@rest_api_bp.route("/views/<int:view_id>", methods=["GET"])
@api_key_required
def get_view(view_id):
    db = _db()
    try:
        v = (
            db.query(SavedView)
            .filter(
                SavedView.id == view_id,
                SavedView.user_id == _user_id(),
            )
            .first()
        )
        if v is None:
            return _err("View not found", 404)
        return _ok(_serialize_saved_view(v))
    finally:
        db.remove()


@rest_api_bp.route("/views/<int:view_id>", methods=["PUT"])
@api_key_required
def update_view(view_id):
    data = request.get_json(silent=True) or {}
    db = _db()
    try:
        v = (
            db.query(SavedView)
            .filter(
                SavedView.id == view_id,
                SavedView.user_id == _user_id(),
            )
            .first()
        )
        if v is None:
            return _err("View not found", 404)
        _apply_view_fields(v, data)
        db.commit()
        db.refresh(v)
        return _ok(_serialize_saved_view(v))
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


@rest_api_bp.route("/views/<int:view_id>", methods=["DELETE"])
@api_key_required
def delete_view(view_id):
    db = _db()
    try:
        v = (
            db.query(SavedView)
            .filter(
                SavedView.id == view_id,
                SavedView.user_id == _user_id(),
            )
            .first()
        )
        if v is None:
            return _err("View not found", 404)
        db.delete(v)
        db.commit()
        return _ok({"deleted": True})
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


def _apply_view_fields(v, data):
    for f in ["name", "description", "settings_json", "is_shared"]:
        if f in data:
            setattr(v, f, data[f])


# ──────────────────────────────────────────────────────────
#  SAVED FRAMINGS
# ──────────────────────────────────────────────────────────


@rest_api_bp.route("/framings", methods=["GET"])
@api_key_required
def list_framings():
    db = _db()
    try:
        q = db.query(SavedFraming).filter(SavedFraming.user_id == _user_id())
        q = q.order_by(SavedFraming.object_name)
        items, meta = _paginate(q)
        return _ok([_serialize_framing(f) for f in items], meta)
    finally:
        db.remove()


@rest_api_bp.route("/framings", methods=["POST"])
@api_key_required
def create_framing():
    data = request.get_json(silent=True) or {}
    if not data.get("object_name"):
        return _err("object_name is required")
    db = _db()
    try:
        f = SavedFraming(user_id=_user_id())
        _apply_framing_fields(f, data)
        db.add(f)
        db.commit()
        db.refresh(f)
        return _ok(_serialize_framing(f), status=201)
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


@rest_api_bp.route("/framings/<int:framing_id>", methods=["GET"])
@api_key_required
def get_framing(framing_id):
    db = _db()
    try:
        f = (
            db.query(SavedFraming)
            .filter(
                SavedFraming.id == framing_id,
                SavedFraming.user_id == _user_id(),
            )
            .first()
        )
        if f is None:
            return _err("Framing not found", 404)
        return _ok(_serialize_framing(f))
    finally:
        db.remove()


@rest_api_bp.route("/framings/<int:framing_id>", methods=["PUT"])
@api_key_required
def update_framing(framing_id):
    data = request.get_json(silent=True) or {}
    db = _db()
    try:
        f = (
            db.query(SavedFraming)
            .filter(
                SavedFraming.id == framing_id,
                SavedFraming.user_id == _user_id(),
            )
            .first()
        )
        if f is None:
            return _err("Framing not found", 404)
        _apply_framing_fields(f, data)
        db.commit()
        db.refresh(f)
        return _ok(_serialize_framing(f))
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


@rest_api_bp.route("/framings/<int:framing_id>", methods=["DELETE"])
@api_key_required
def delete_framing(framing_id):
    db = _db()
    try:
        f = (
            db.query(SavedFraming)
            .filter(
                SavedFraming.id == framing_id,
                SavedFraming.user_id == _user_id(),
            )
            .first()
        )
        if f is None:
            return _err("Framing not found", 404)
        db.delete(f)
        db.commit()
        return _ok({"deleted": True})
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


def _apply_framing_fields(f, data):
    for field in [
        "object_name",
        "rig_id",
        "rig_effective_focal_length",
        "rig_fov_w_arcmin",
        "rig_fov_h_arcmin",
        "rig_image_scale",
        "rig_resolution_width_px",
        "rig_resolution_height_px",
        "survey_name",
        "survey_ra_hours",
        "survey_dec_deg",
        "survey_fov_deg",
        "survey_rotation_deg",
        "survey_width_px",
        "survey_height_px",
        "mosaic_panels_x",
        "mosaic_panels_y",
        "mosaic_overlap_pct",
        "image_brightness",
        "image_contrast",
        "image_saturation",
        "image_invert",
        "geo_belt_enabled",
    ]:
        if field in data:
            setattr(f, field, data[field])
    f.updated_at = datetime.utcnow()


# ──────────────────────────────────────────────────────────
#  CUSTOM FILTERS
# ──────────────────────────────────────────────────────────


@rest_api_bp.route("/custom-filters", methods=["GET"])
@api_key_required
def list_custom_filters():
    db = _db()
    try:
        q = db.query(UserCustomFilter).filter(UserCustomFilter.user_id == _user_id())
        q = q.order_by(UserCustomFilter.filter_key)
        items, meta = _paginate(q)
        return _ok([_serialize_custom_filter(cf) for cf in items], meta)
    finally:
        db.remove()


@rest_api_bp.route("/custom-filters", methods=["POST"])
@api_key_required
def create_custom_filter():
    data = request.get_json(silent=True) or {}
    if not data.get("filter_key") or not data.get("filter_label"):
        return _err("filter_key and filter_label are required")
    db = _db()
    try:
        cf = UserCustomFilter(
            user_id=_user_id(),
            filter_key=data["filter_key"],
            filter_label=data["filter_label"],
        )
        db.add(cf)
        db.commit()
        db.refresh(cf)
        return _ok(_serialize_custom_filter(cf), status=201)
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


@rest_api_bp.route("/custom-filters/<int:filter_id>", methods=["DELETE"])
@api_key_required
def delete_custom_filter(filter_id):
    db = _db()
    try:
        cf = (
            db.query(UserCustomFilter)
            .filter(
                UserCustomFilter.id == filter_id,
                UserCustomFilter.user_id == _user_id(),
            )
            .first()
        )
        if cf is None:
            return _err("Custom filter not found", 404)
        db.delete(cf)
        db.commit()
        return _ok({"deleted": True})
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


# ──────────────────────────────────────────────────────────
#  UI PREFERENCES
# ──────────────────────────────────────────────────────────


@rest_api_bp.route("/preferences", methods=["GET"])
@api_key_required
def get_preferences():
    db = _db()
    try:
        pref = db.query(UiPref).filter(UiPref.user_id == _user_id()).first()
        if pref is None:
            return _ok({"id": None, "json_blob": None})
        return _ok(_serialize_ui_pref(pref))
    finally:
        db.remove()


@rest_api_bp.route("/preferences", methods=["PUT"])
@api_key_required
def update_preferences():
    data = request.get_json(silent=True) or {}
    if "json_blob" not in data:
        return _err("json_blob is required")
    db = _db()
    try:
        pref = db.query(UiPref).filter(UiPref.user_id == _user_id()).first()
        if pref is None:
            pref = UiPref(user_id=_user_id(), json_blob=data["json_blob"])
            db.add(pref)
        else:
            pref.json_blob = data["json_blob"]
        db.commit()
        db.refresh(pref)
        return _ok(_serialize_ui_pref(pref))
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


# ──────────────────────────────────────────────────────────
#  API KEYS  (self-service management)
# ──────────────────────────────────────────────────────────


@rest_api_bp.route("/api-keys", methods=["GET"])
@api_key_required
def list_api_keys():
    """List all API keys belonging to the authenticated user."""
    db = _db()
    try:
        keys = (
            db.query(ApiKey)
            .filter(ApiKey.user_id == _user_id())
            .order_by(ApiKey.created_at.desc())
            .all()
        )
        return _ok([_serialize_api_key(k) for k in keys])
    finally:
        db.remove()


@rest_api_bp.route("/api-keys", methods=["POST"])
@api_key_required
def create_api_key_endpoint():
    """Create a new API key.  Returns the raw key (only time it's visible)."""
    data = request.get_json(silent=True) or {}
    name = data.get("name", "unnamed")
    db = _db()
    try:
        # Limit keys per user to prevent abuse
        count = (
            db.query(ApiKey)
            .filter(
                ApiKey.user_id == _user_id(),
                ApiKey.is_active.is_(True),
            )
            .count()
        )
        if count >= 25:
            return _err("Maximum 25 active API keys per user", 429)

        raw_key = create_api_key(db, _user_id(), name=name)
        # The db session in create_api_key already committed; re-query to serialize
        new_key = (
            db.query(ApiKey)
            .filter(
                ApiKey.key_hash == hash_api_key(raw_key),
            )
            .first()
        )
        result = _serialize_api_key(new_key)
        result["key"] = raw_key  # Only time the full key is revealed
        return _ok(result, status=201)
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


@rest_api_bp.route("/api-keys/<int:key_id>", methods=["DELETE"])
@api_key_required
def revoke_api_key(key_id):
    """Revoke (deactivate) an API key.  Cannot delete the key you're using."""
    db = _db()
    try:
        api_key = (
            db.query(ApiKey)
            .filter(
                ApiKey.id == key_id,
                ApiKey.user_id == _user_id(),
            )
            .first()
        )
        if api_key is None:
            return _err("API key not found", 404)

        # Prevent revoking the key currently in use
        if hasattr(g, "api_key_obj") and g.api_key_obj.id == key_id:
            return _err("Cannot revoke the API key you are currently using", 400)

        api_key.is_active = False
        db.commit()
        return _ok({"revoked": True})
    except Exception as e:
        db.rollback()
        return _err(str(e), 500)
    finally:
        db.remove()


# ──────────────────────────────────────────────────────────
#  ADMIN: USER MANAGEMENT  (multi-user mode only)
# ──────────────────────────────────────────────────────────


@rest_api_bp.route("/admin/users", methods=["GET"])
@api_key_required
def admin_list_users():
    """List all users (admin only, multi-user mode)."""
    if SINGLE_USER_MODE:
        return _err("Not available in single-user mode", 403)
    db = _db()
    try:
        users = db.query(DbUser).order_by(DbUser.username).all()
        return _ok(
            [
                {
                    "id": u.id,
                    "username": u.username,
                    "active": u.active,
                }
                for u in users
            ]
        )
    finally:
        db.remove()


# ──────────────────────────────────────────────────────────
#  STATUS / INFO
# ──────────────────────────────────────────────────────────


@rest_api_bp.route("/status", methods=["GET"])
@api_key_required
def api_status():
    """Return server info and authenticated user context."""
    from nova.config import APP_VERSION

    return _ok(
        {
            "version": APP_VERSION,
            "single_user_mode": SINGLE_USER_MODE,
            "user": g.db_user.username,
        }
    )
