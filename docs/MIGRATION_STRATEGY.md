# Estrategia de Migración (Migration Strategy)

Este documento describe el plan de transición desde la API clásica v2.x (basada en interfaces y mapas dinámicos) hacia la nueva API Zero-Allocation (basada en el Builder Pattern), introducida como puente en v2.4.x o v2.5.x y consolidada en una futura y posible versión v3.0.0.

---

## 1. Principio de No Disrupción

Para evitar romper las integraciones existentes de los clientes de Acacia de forma inmediata:
1. **Mantener la API Pública Actual:** Los métodos tradicionales como `Info(data interface{}, args ...interface{})` seguirán funcionando como una capa de compatibilidad heredada (legacy v2.x).
2. **Implementación de Coexistencia:** La nueva interfaz fluida basada en builders coexistirá junto a la interfaz clásica en el mismo paquete a partir de las versiones puente v2.4.x / v2.5.x.

---

## 2. Fase de Transición (Coexistencia)

### API Clásica (Legacy - v2.x)
* **Comportamiento:** Internamente, el logger adaptará la llamada clásica a un buffer temporal del pool de eventos. Aunque seguirá teniendo asignaciones en el Heap por culpa del boxing de `interface{}`, su implementación interna se optimizará para evitar el uso de `fmt` y reflexión pesada donde sea posible.
* **Estado:** Depreciado (Deprecated).

### API Fluida (Recomendada - Builder v2.4.x+ / Posible v3)
* **Comportamiento:** Llamadas optimizadas de alto rendimiento sin allocations directas.
* **Uso:** Recomendado para nuevas implementaciones y hot paths críticos.

---

## 3. Cronograma de Depreciación y Cambios

1. **Versiones puente v2.4.x / v2.5.x (Fase 00 a Fase 07):**
   * Coexistencia de ambas APIs.
   * Marcado de métodos clásicos como `Deprecated` en la documentación de Go.
2. **Candidato a Versión 3.0.0 (Fase 08):**
   * Eliminación definitiva de las firmas de interfaz dinámicas del núcleo principal si se opta por el corte limpio.
   * La API clásica se moverá a un paquete opcional de compatibilidad (`github.com/humanjuan/acacia/v2/compat`) o se eliminará para garantizar que el logger permanezca 100% Zero-Allocation.
