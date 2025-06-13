# Makefile
# Commandes utilitaires Synergo

.PHONY: help setup start stop test-hfsql

help:
	@echo "🚀 Commandes Synergo disponibles:"
	@echo ""
	@echo "  setup        - Installation initiale complète"
	@echo "  start        - Démarrer les services Docker"
	@echo "  stop         - Arrêter les services"
	@echo "  test-hfsql   - Tester la connexion HFSQL"
	@echo "  logs         - Voir les logs"
	@echo "  clean        - Nettoyer les containers et volumes"
	@echo ""

setup:
	@echo "🏗️  Installation Synergo..."
	@mkdir -p backend/app/{core,models,api/v1,services,utils,sync/{strategies,transformers,connectors}}
	@mkdir -p frontend/src/{components/{common,dashboard,sync},pages,services,utils,hooks}
	@mkdir -p database/{migrations,seeds}
	@mkdir -p docker logs
	@cp backend/.env.example backend/.env
	@echo "✅ Structure créée. Modifiez backend/.env puis lancez 'make start'"

start:
	@echo "🚀 Démarrage des services Synergo..."
	@cd docker && docker-compose up -d
	@echo "✅ Services démarrés:"
	@echo "   - PostgreSQL: localhost:5432"
	@echo "   - Redis: localhost:6379" 
	@echo "   - PgAdmin: http://localhost:8080"

stop:
	@echo "⏹️  Arrêt des services..."
	@cd docker && docker-compose down

test-hfsql:
	@echo "🔌 Test connexion HFSQL..."
	@cd scripts && python test_hfsql_connection.py

logs:
	@cd docker && docker-compose logs -f

clean:
	@echo "🧹 Nettoyage..."
	@cd docker && docker-compose down -v
	@docker system prune -f