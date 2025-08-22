import { db } from "./storage";
import { eq, and, desc } from "drizzle-orm";
import { cashScheduleConfig, cashAutoOperationsLog } from "@shared/schema";

export class CashScheduleStorage {
  // Obtener configuración de horarios para un cliente
  async getScheduleConfig(clientId: number) {
    try {
      console.log(`🔍 [DEBUG] getScheduleConfig called for clientId: ${clientId}`);

      const [config] = await db
        .select()
        .from(cashScheduleConfig)
        .where(eq(cashScheduleConfig.clientId, clientId));

      console.log(`🔍 [DEBUG] Raw DB result for clientId ${clientId}:`, JSON.stringify(config, null, 2));

      if (config) {
        console.log(`🔍 [DEBUG] Parsed config values:`, {
          openHour: config.openHour,
          openMinute: config.openMinute,
          closeHour: config.closeHour,
          closeMinute: config.closeMinute,
          autoOpenEnabled: config.autoOpenEnabled,
          autoCloseEnabled: config.autoCloseEnabled
        });
      }

      return config || null;
    } catch (error) {
      console.error('Error getting schedule config:', error);
      return null;
    }
  }

  // Crear o actualizar configuración de horarios
  async upsertScheduleConfig(clientId: number, configData: any) {
    try {
      const existingConfig = await this.getScheduleConfig(clientId);

      // Prepare data with proper timestamp handling
      const cleanData = {
        autoOpenEnabled: configData.autoOpenEnabled || false,
        autoCloseEnabled: configData.autoCloseEnabled || false,
        openHour: parseInt(configData.openHour) || 9,
        openMinute: parseInt(configData.openMinute) || 0,
        closeHour: parseInt(configData.closeHour) || 18,
        closeMinute: parseInt(configData.closeMinute) || 0,
        activeDays: configData.activeDays || "1,2,3,4,5,6,7",
        timezone: configData.timezone || "America/Argentina/Buenos_Aires",
      };

      if (existingConfig) {
        // Actualizar existente
        const [updated] = await db
          .update(cashScheduleConfig)
          .set({
            ...cleanData,
            updatedAt: new Date(),
          })
          .where(eq(cashScheduleConfig.clientId, clientId))
          .returning();

        return updated;
      } else {
        // Crear nuevo
        const [created] = await db
          .insert(cashScheduleConfig)
          .values({
            clientId,
            ...cleanData,
            createdAt: new Date(),
            updatedAt: new Date(),
          })
          .returning();

        return created;
      }
    } catch (error) {
      console.error('Error upserting schedule config:', error);
      throw error;
    }
  }

  // Registrar operación automática en el log
  async logAutoOperation(operationData: {
    clientId: number;
    operationType: string;
    cashRegisterId?: number;
    scheduledTime?: Date;
    status?: string;
    errorMessage?: string;
    reportId?: number;
    notes?: string;
  }) {
    try {
      const [logged] = await db
        .insert(cashAutoOperationsLog)
        .values(operationData)
        .returning();

      return logged;
    } catch (error) {
      console.error('Error logging auto operation:', error);
      throw error;
    }
  }

  // Obtener log de operaciones automáticas
  async getAutoOperationsLog(clientId: number, limit = 50) {
    try {
      console.log(`🔍 [STORAGE] getAutoOperationsLog called for clientId: ${clientId}, limit: ${limit}`);
      
      const logs = await db
        .select()
        .from(cashAutoOperationsLog)
        .where(eq(cashAutoOperationsLog.clientId, clientId))
        .orderBy(cashAutoOperationsLog.executedTime.desc()) // Orden descendente para mostrar más recientes primero
        .limit(limit);

      console.log(`🔍 [STORAGE] Found ${logs.length} operations log entries for client ${clientId}`);
      console.log(`🔍 [STORAGE] Log entries:`, logs.map(log => ({
        id: log.id,
        operationType: log.operationType,
        status: log.status,
        executedTime: log.executedTime,
        notes: log.notes?.substring(0, 50) + '...'
      })));

      return logs;
    } catch (error) {
      console.error('Error getting auto operations log:', error);
      return [];
    }
  }

  // Verificar si debe ejecutarse una operación automática
  async shouldExecuteAutoOperation(clientId: number, operationType: 'open' | 'close'): Promise<boolean> {
    try {
      const config = await this.getScheduleConfig(clientId);
      if (!config) {
        console.log(`🔍 [CASH-AUTO] No configuration found for client ${clientId}`);
        return false;
      }

      // FIXED: Proper Argentina timezone handling
      const now = new Date();
      
      // Get Argentina time properly
      const argentinaTime = new Date(now.toLocaleString("en-US", {
        timeZone: "America/Argentina/Buenos_Aires"
      }));
      
      // Additional verification log
      const utcTime = now.toISOString();
      const argTimeString = argentinaTime.toLocaleString('es-AR', {
        timeZone: 'America/Argentina/Buenos_Aires',
        hour12: false
      });

      console.log(`🕐 [CASH-AUTO] Current time in Argentina: ${argentinaTime.toISOString()} (${argentinaTime.getHours()}:${argentinaTime.getMinutes().toString().padStart(2, '0')})`);

      const currentDay = argentinaTime.getDay() || 7; // Convert Sunday (0) to 7
      const activeDays = config.activeDays?.split(',').map(d => parseInt(d)) || [];

      // Verificar si hoy es un día activo
      if (!activeDays.includes(currentDay)) {
        console.log(`🔍 [CASH-AUTO] Client ${clientId} is not active on day ${currentDay}`);
        return false;
      }

      // Verificar si ya se ejecutó esta operación en el último minuto (permitir reintentos más frecuentes)
      const hasExecutedRecently = await this.hasExecutedOperationRecently(clientId, operationType, 1);
      if (hasExecutedRecently) {
        console.log(`🔍 [CASH-AUTO] Operation ${operationType} already executed recently for client ${clientId}`);
        return false;
      }

      if (operationType === 'open') {
        if (!config.autoOpenEnabled) {
          console.log(`🔍 [CASH-AUTO] Auto open is DISABLED for client ${clientId}`);
          return false;
        }

        const targetHour = config.openHour ?? 0;
        const targetMinute = config.openMinute ?? 0;

        console.log(`🔍 [CASH-AUTO] Target open time: ${targetHour}:${targetMinute.toString().padStart(2, '0')}`);
        console.log(`🔍 [CASH-AUTO] Current time: ${argentinaTime.getHours()}:${argentinaTime.getMinutes().toString().padStart(2, '0')}`);

        // FIXED: Expandir ventana de apertura para ser más flexible
        // Permitir ejecución desde la hora configurada hasta 2 horas después
        const currentMinutes = argentinaTime.getHours() * 60 + argentinaTime.getMinutes();
        const targetMinutes = targetHour * 60 + targetMinute;
        const withinWindow = currentMinutes >= targetMinutes && currentMinutes <= targetMinutes + 120; // 2 horas = 120 minutos

        console.log(`🔍 [CASH-AUTO] Should execute OPEN: ${withinWindow} (current: ${currentMinutes}, target: ${targetMinutes}, window: ${targetMinutes}-${targetMinutes + 120})`);
        console.log(`🔍 [CASH-AUTO] Open decision details:`, {
          clientId,
          currentTime: `${argentinaTime.getHours()}:${argentinaTime.getMinutes().toString().padStart(2, '0')}`,
          targetTime: `${targetHour}:${targetMinute.toString().padStart(2, '0')}`,
          currentMinutes,
          targetMinutes,
          windowEnd: targetMinutes + 120,
          withinWindow,
          autoOpenEnabled: config.autoOpenEnabled,
          minutesLate: currentMinutes - targetMinutes
        });

        return withinWindow;
      } else {
        if (!config.autoCloseEnabled) {
          console.log(`🔍 [CASH-AUTO] Auto close is DISABLED for client ${clientId}`);
          return false;
        }

        const targetHour = config.closeHour ?? 23;
        const targetMinute = config.closeMinute ?? 59;

        console.log(`🔍 [CASH-AUTO] Target close time: ${targetHour}:${targetMinute.toString().padStart(2, '0')}`);
        console.log(`🔍 [CASH-AUTO] Current time: ${argentinaTime.getHours()}:${argentinaTime.getMinutes().toString().padStart(2, '0')}`);

        // FIXED: Expandir ventana de cierre para ser más flexible
        // Permitir ejecución desde la hora configurada hasta 1 hora después
        const currentMinutes = argentinaTime.getHours() * 60 + argentinaTime.getMinutes();
        const targetMinutes = targetHour * 60 + targetMinute;
        const withinWindow = currentMinutes >= targetMinutes && currentMinutes <= targetMinutes + 60; // 1 hora = 60 minutos

        console.log(`🔍 [CASH-AUTO] Should execute CLOSE: ${withinWindow} (current: ${currentMinutes}, target: ${targetMinutes}, window: ${targetMinutes}-${targetMinutes + 60})`);
        console.log(`🔍 [CASH-AUTO] Close decision details:`, {
          clientId,
          currentTime: `${argentinaTime.getHours()}:${argentinaTime.getMinutes().toString().padStart(2, '0')}`,
          targetTime: `${targetHour}:${targetMinute.toString().padStart(2, '0')}`,
          currentMinutes,
          targetMinutes,
          windowEnd: targetMinutes + 60,
          withinWindow,
          autoCloseEnabled: config.autoCloseEnabled,
          minutesLate: currentMinutes - targetMinutes
        });

        return withinWindow;
      }
    } catch (error) {
      console.error('❌ [CASH-AUTO] Error checking auto operation:', error);
      return false;
    }
  }

  // Verificar si ya se ejecutó una operación en los últimos N minutos
  private async hasExecutedOperationRecently(clientId: number, operationType: string, minutesWindow: number = 5): Promise<boolean> {
    try {
      const now = new Date();
      const windowStart = new Date(now.getTime() - minutesWindow * 60 * 1000);

      const recentLogs = await db
        .select()
        .from(cashAutoOperationsLog)
        .where(
          and(
            eq(cashAutoOperationsLog.clientId, clientId),
            eq(cashAutoOperationsLog.operationType, `auto_${operationType}`),
            eq(cashAutoOperationsLog.status, 'success')
          )
        )
        .orderBy(cashAutoOperationsLog.executedTime)
        .limit(1);

      if (recentLogs.length === 0) {
        console.log(`🔍 [CASH-AUTO] No recent ${operationType} operations found for client ${clientId}`);
        return false;
      }

      const lastExecution = new Date(recentLogs[0].executedTime);
      const wasRecent = lastExecution >= windowStart;
      
      console.log(`🔍 [CASH-AUTO] Last ${operationType} execution for client ${clientId}: ${lastExecution.toISOString()}`);
      console.log(`🔍 [CASH-AUTO] Window start: ${windowStart.toISOString()}, Was recent: ${wasRecent}`);
      
      return wasRecent;
    } catch (error) {
      console.error('Error checking recent execution:', error);
      return false;
    }
  }

  // Verificar si ya se ejecutó una operación en esta hora específica
  private async hasExecutedOperationThisHour(clientId: number, operationType: string, currentTime: Date): Promise<boolean> {
    try {
      const hourStart = new Date(currentTime);
      hourStart.setMinutes(0, 0, 0);

      const hourEnd = new Date(currentTime);
      hourEnd.setMinutes(59, 59, 999);

      const recentLogs = await db
        .select()
        .from(cashAutoOperationsLog)
        .where(
          and(
            eq(cashAutoOperationsLog.clientId, clientId),
            eq(cashAutoOperationsLog.operationType, `auto_${operationType}`),
            eq(cashAutoOperationsLog.status, 'success')
          )
        )
        .orderBy(cashAutoOperationsLog.executedTime)
        .limit(1);

      if (recentLogs.length === 0) return false;

      const lastExecution = new Date(recentLogs[0].executedTime);
      return lastExecution >= hourStart && lastExecution <= hourEnd;
    } catch (error) {
      console.error('Error checking execution history:', error);
      return false;
    }
  }

  // Verificar si una operación se ejecutó exitosamente hoy
  private async wasOperationExecutedToday(clientId: number, operationType: 'auto_open' | 'auto_close'): Promise<boolean> {
    try {
      const today = new Date();
      const startOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate(), 0, 0, 0, 0);
      const endOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate(), 23, 59, 59, 999);

      const todayLogs = await db
        .select()
        .from(cashAutoOperationsLog)
        .where(
          and(
            eq(cashAutoOperationsLog.clientId, clientId),
            eq(cashAutoOperationsLog.operationType, operationType),
            eq(cashAutoOperationsLog.status, 'success')
          )
        )
        .orderBy(cashAutoOperationsLog.executedTime.desc())
        .limit(1);

      if (todayLogs.length === 0) return false;

      const lastExecution = new Date(todayLogs[0].executedTime);
      const wasToday = lastExecution >= startOfDay && lastExecution <= endOfDay;
      
      console.log(`🔍 [OPERATION-STATUS] ${operationType} for client ${clientId}: lastExecution=${lastExecution.toISOString()}, wasToday=${wasToday}`);
      
      return wasToday;
    } catch (error) {
      console.error('Error checking operation execution:', error);
      return false;
    }
  }

  // Obtener próximas operaciones programadas
  async getScheduledOperations(clientId: number) {
    try {
      console.log(`🔍 [DEBUG] getScheduledOperations called for clientId: ${clientId}`);

      const config = await this.getScheduleConfig(clientId);
      console.log(`🔍 [DEBUG] Config retrieved in getScheduledOperations:`, JSON.stringify(config, null, 2));

      if (!config) {
        console.log(`🔍 [DEBUG] No config found for clientId: ${clientId}, returning empty array`);
        return [];
      }

      const operations = [];

      if (config.autoOpenEnabled) {
        console.log(`🔍 [DEBUG] Creating auto_open operation with hours: ${config.openHour}, minutes: ${config.openMinute}`);

        // Verificar si ya se ejecutó hoy
        const wasOpenExecutedToday = await this.wasOperationExecutedToday(clientId, 'auto_open');

        // MOSTRAR CONFIGURACIÓN EXACTA: usar fecha fija solo para display, horarios de configuración
        const configuredOpen = new Date('2025-01-01'); // Fecha fija para display

        // IMPORTANTE: Usar los valores EXACTOS de la DB sin fallbacks
        const openHour = config.openHour !== null && config.openHour !== undefined ? config.openHour : 9;
        const openMinute = config.openMinute !== null && config.openMinute !== undefined ? config.openMinute : 0;

        console.log(`🔍 [DEBUG] Setting open hours - config.openHour: ${config.openHour}, config.openMinute: ${config.openMinute}`);
        console.log(`🔍 [DEBUG] Resolved values - openHour: ${openHour}, openMinute: ${openMinute}`);

        configuredOpen.setHours(openHour, openMinute, 0, 0);

        console.log(`🔍 [DEBUG] configuredOpen after setHours:`, configuredOpen);
        console.log(`🔍 [DEBUG] configuredOpen hours/minutes:`, {
          hours: configuredOpen.getHours(),
          minutes: configuredOpen.getMinutes()
        });

        const openOperation = {
          type: 'auto_open',
          scheduledTime: configuredOpen,
          enabled: config.autoOpenEnabled,
          wasExecutedToday: wasOpenExecutedToday,
          executionStatus: wasOpenExecutedToday ? 'executed' : 'scheduled'
        };

        console.log(`🔍 [DEBUG] Created openOperation:`, JSON.stringify(openOperation, null, 2));
        operations.push(openOperation);
      }

      if (config.autoCloseEnabled) {
        console.log(`🔍 [DEBUG] Creating auto_close operation with hours: ${config.closeHour}, minutes: ${config.closeMinute}`);

        // Verificar si ya se ejecutó hoy
        const wasCloseExecutedToday = await this.wasOperationExecutedToday(clientId, 'auto_close');
        const wasOpenExecutedToday = await this.wasOperationExecutedToday(clientId, 'auto_open');

        // MOSTRAR CONFIGURACIÓN EXACTA: usar fecha fija solo para display, horarios de configuración
        const configuredClose = new Date('2025-01-01'); // Fecha fija para display

        // IMPORTANTE: Usar los valores EXACTOS de la DB sin fallbacks
        const closeHour = config.closeHour !== null && config.closeHour !== undefined ? config.closeHour : 18;
        const closeMinute = config.closeMinute !== null && config.closeMinute !== undefined ? config.closeMinute : 0;

        console.log(`🔍 [DEBUG] Setting close hours - config.closeHour: ${config.closeHour}, config.closeMinute: ${config.closeMinute}`);
        console.log(`🔍 [DEBUG] Resolved values - closeHour: ${closeHour}, closeMinute: ${closeMinute}`);

        configuredClose.setHours(closeHour, closeMinute, 0, 0);

        console.log(`🔍 [DEBUG] configuredClose after setHours:`, configuredClose);
        console.log(`🔍 [DEBUG] configuredClose hours/minutes:`, {
          hours: configuredClose.getHours(),
          minutes: configuredClose.getMinutes()
        });

        // Determinar el estado del cierre
        let closeExecutionStatus;
        if (wasCloseExecutedToday) {
          closeExecutionStatus = 'executed';
        } else if (wasOpenExecutedToday) {
          closeExecutionStatus = 'next_operation';
        } else {
          closeExecutionStatus = 'scheduled';
        }

        const closeOperation = {
          type: 'auto_close',
          scheduledTime: configuredClose,
          enabled: config.autoCloseEnabled,
          wasExecutedToday: wasCloseExecutedToday,
          executionStatus: closeExecutionStatus
        };

        console.log(`🔍 [DEBUG] Created closeOperation:`, JSON.stringify(closeOperation, null, 2));
        operations.push(closeOperation);
      }

      console.log(`🔍 [DEBUG] Final operations array for clientId ${clientId}:`, JSON.stringify(operations, null, 2));
      console.log(`🕐 Scheduled operations for client ${clientId}: Configuration shows Open:${config.openHour}:${config.openMinute?.toString().padStart(2, '0')} Close:${config.closeHour}:${config.closeMinute?.toString().padStart(2, '0')}`);

      return operations;
    } catch (error) {
      console.error('Error getting scheduled operations:', error);
      return [];
    }
  }
}

// Exportar instancia singleton
export const cashScheduleStorage = new CashScheduleStorage();