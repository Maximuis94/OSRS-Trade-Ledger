import click
from pathlib import Path
from .project_setup import ProjectConfig, ProjectSetup

@click.group()
def cli():
    """OSRS Trade Ledger setup tools"""
    pass

@cli.command()
@click.option('--root-dir', type=click.Path(), help='Project root directory')
@click.option('--force/--no-force', default=False, help='Force setup even if directories exist')
def setup(root_dir, force):
    """Run the complete project setup"""
    if root_dir:
        root_dir = Path(root_dir)
    else:
        root_dir = Path(__file__).parent.parent.parent

    if not force and any((root_dir / 'data').glob('*')):
        if not click.confirm('Project directories already exist. Do you want to continue?'):
            return

    config = ProjectConfig(
        root_dir=root_dir,
        data_dir=root_dir / 'data',
        database_dir=root_dir / 'data' / 'db',
        resource_dir=root_dir / 'data' / 'resources',
        export_dir=root_dir / 'data' / 'export',
        log_dir=root_dir / 'data' / 'logs',
        temp_dir=root_dir / 'data' / 'temp'
    )

    setup = ProjectSetup(config)
    
    with click.progressbar(length=4, label='Setting up project') as bar:
        setup.setup_project_structure()
        bar.update(1)
        
        setup.setup_databases()
        bar.update(1)
        
        setup.create_config_files()
        bar.update(1)
        
        click.echo('\nSetup completed successfully!')

@cli.command()
@click.option('--backup-dir', type=click.Path(), required=True, help='Backup directory')
def backup(backup_dir):
    """Backup project databases and configuration"""
    backup_dir = Path(backup_dir)
    backup_dir.mkdir(parents=True, exist_ok=True)
    
    # Implementation of backup logic here
    click.echo(f'Backing up to {backup_dir}')

@cli.command()
def verify():
    """Verify project setup and database integrity"""
    # Implementation of verification logic here
    click.echo('Verifying project setup...')

if __name__ == '__main__':
    cli() 