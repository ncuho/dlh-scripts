"""Run inside the Superset web container. Scope migration authoring to database 2."""
import json
from superset.app import create_app
app=create_app()
with app.app_context():
    from superset import security_manager as sm, db
    from superset.models.core import Database
    user=sm.find_user(username='oil_migration_publisher')
    if user is None:raise RuntimeError('configured publisher identity is missing')
    role=sm.find_role('OilMigrationPublisher')
    if role is None:raise RuntimeError('publisher role is missing')
    database=db.session.get(Database,2)
    allowed={(verb,entity) for entity in ('Chart','Dataset','Dashboard') for verb in ('can_read','can_write')}
    allowed|={('can_read','Database'),('can_read','SecurityRestApi'),('can_explore_json','Superset'),('can_slice','Superset'),('can_explore','Superset'),('can_dashboard','Superset'),('database_access',database.perm)}
    role.permissions=[p for p in db.session.query(sm.permissionview_model).all() if (p.permission.name,p.view_menu.name) in allowed]
    if role not in user.roles:user.roles.append(role)
    db.session.commit()
    print(json.dumps({'role':role.name,'database_id':2,'permissions':len(role.permissions)}))
