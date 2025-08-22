import { useState, useEffect } from "react";
import { useAuth } from "@/hooks/use-auth";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import Header from "@/components/layout/header";
import Sidebar from "@/components/layout/sidebar";
import MobileNav from "@/components/layout/mobile-nav";
import Footer from "@/components/layout/footer";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { AlertCircle, Clock, Calendar, CheckCircle, XCircle, Settings, Save, RefreshCw } from "lucide-react";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { useToast } from "@/hooks/use-toast";

export default function CashScheduleConfig() {
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const { user } = useAuth();
  const { toast } = useToast();
  const queryClient = useQueryClient();

  const [config, setConfig] = useState({
    autoOpenEnabled: false,
    autoCloseEnabled: false,
    openHour: 9,
    openMinute: 0,
    closeHour: 18,
    closeMinute: 0,
    activeDays: "1,2,3,4,5,6,7",
    timezone: "America/Argentina/Buenos_Aires",
  });

  // Get current configuration
  const { data: currentConfig, isLoading } = useQuery({
    queryKey: ["/api/cash-schedule/config", user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/cash-schedule/config?clientId=${user?.clientId}`);
      if (!response.ok) throw new Error('Failed to fetch configuration');
      return response.json();
    },
    enabled: !!user?.clientId,
  });

  // Get automation service status
  const { data: serviceStatus } = useQuery({
    queryKey: ["/api/cash-schedule/service-status"],
    queryFn: async () => {
      const response = await fetch("/api/cash-schedule/service-status");
      if (!response.ok) throw new Error('Failed to fetch service status');
      return response.json();
    },
    refetchInterval: 10000, // Refresh every 10 seconds
  });

  // Get operations log
  const { data: operationsLog } = useQuery({
    queryKey: ["/api/cash-schedule/log", user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/cash-schedule/log?clientId=${user?.clientId}&limit=20`);
      if (!response.ok) throw new Error('Failed to fetch operations log');
      return response.json();
    },
    enabled: !!user?.clientId,
  });

  // Get scheduled operations with execution status
  const { data: scheduledOperations } = useQuery({
    queryKey: ["/api/cash-schedule/operations", user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/cash-schedule/operations?clientId=${user?.clientId}`);
      if (!response.ok) throw new Error('Failed to fetch scheduled operations');
      return response.json();
    },
    enabled: !!user?.clientId,
    refetchInterval: 10000, // Refresh every 10 seconds for real-time updates
  });

  // Update configuration mutation
  const updateConfigMutation = useMutation({
    mutationFn: async (newConfig: any) => {
      const response = await fetch("/api/cash-schedule/config", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-user-id": user?.id?.toString() || "",
        },
        body: JSON.stringify({
          clientId: user?.clientId,
          ...newConfig,
        }),
      });

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.message || 'Failed to update configuration');
      }

      return response.json();
    },
    onSuccess: () => {
      toast({
        title: "Configuración guardada",
        description: "Los horarios de caja han sido actualizados exitosamente.",
      });
      // Invalidar queries relacionadas con horarios
      queryClient.invalidateQueries({ queryKey: ["/api/cash-schedule/config"] });
      queryClient.invalidateQueries({ queryKey: ["/api/cash-schedule/log"] });
      queryClient.invalidateQueries({ queryKey: ["/api/cash-schedule/operations"] });
    },
    onError: (error: any) => {
      toast({
        variant: "destructive",
        title: "Error al guardar",
        description: error.message || "No se pudo actualizar la configuración.",
      });
    },
  });

  // Set initial config when data loads
  useEffect(() => {
    if (currentConfig) {
      setConfig(currentConfig);
    }
  }, [currentConfig]);

  const handleSaveConfig = () => {
    updateConfigMutation.mutate(config);
  };

  const getDayName = (day: string) => {
    const days = {
      '1': 'Lun', '2': 'Mar', '3': 'Mié', '4': 'Jue',
      '5': 'Vie', '6': 'Sáb', '7': 'Dom'
    };
    return days[day as keyof typeof days] || day;
  };

  const formatTime = (hour: number, minute: number) => {
    return `${hour.toString().padStart(2, '0')}:${minute.toString().padStart(2, '0')}`;
  };

  if (isLoading) {
    return (
      <div className="flex h-screen bg-gray-50 dark:bg-gray-900">
        <Sidebar />
        <div className="flex-1 flex flex-col">
          <Header />
          <div className="flex-1 flex items-center justify-center">
            <div className="text-lg">Cargando configuración...</div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 to-blue-50 dark:from-gray-900 dark:to-gray-800 flex flex-col">
      <Header onMobileMenuToggle={() => setMobileNavOpen(true)} />
      <MobileNav open={mobileNavOpen} onOpenChange={setMobileNavOpen} />

      <div className="flex flex-1">
        <Sidebar />

        <main className="flex-1">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">

            {/* Header */}
            <div className="mb-8">
              <div className="flex items-center justify-between">
                <div>
                  <h1 className="text-3xl font-bold text-gray-900 dark:text-white flex items-center">
                    <Clock className="mr-3 h-8 w-8 text-primary" />
                    Configuración de Horarios de Caja
                  </h1>
                  <p className="text-gray-600 dark:text-gray-300 mt-1">
                    Configure la apertura y cierre automático de caja con reportes
                  </p>
                </div>

                {/* Service Status */}
                <div className="flex items-center space-x-2">
                  <div className="flex items-center space-x-2">
                    {serviceStatus?.isRunning ? (
                      <CheckCircle className="h-5 w-5 text-green-500" />
                    ) : (
                      <XCircle className="h-5 w-5 text-red-500" />
                    )}
                    <span className="text-sm font-medium">
                      Servicio: {serviceStatus?.isRunning ? 'Activo' : 'Inactivo'}
                    </span>
                  </div>
                </div>
              </div>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">

              {/* Configuration Panel */}
              <div className="space-y-6">
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center">
                      <Settings className="mr-2 h-5 w-5" />
                      Configuración de Horarios
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-6">

                    {/* Auto Open Configuration */}
                    <div className="space-y-4">
                      <div className="flex items-center justify-between">
                        <div>
                          <h3 className="text-lg font-medium">Apertura Automática</h3>
                          <p className="text-sm text-gray-600">Abrir caja automáticamente</p>
                        </div>
                        <Switch
                          checked={config.autoOpenEnabled}
                          onCheckedChange={(checked) =>
                            setConfig(prev => ({ ...prev, autoOpenEnabled: checked }))
                          }
                        />
                      </div>

                      {config.autoOpenEnabled && (
                        <div className="grid grid-cols-2 gap-4 ml-4 p-4 bg-gray-50 rounded-lg">
                          <div>
                            <Label htmlFor="openHour">Hora</Label>
                            <Select
                              value={config.openHour.toString()}
                              onValueChange={(value) =>
                                setConfig(prev => ({ ...prev, openHour: parseInt(value) }))
                              }
                            >
                              <SelectTrigger>
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent>
                                {Array.from({ length: 24 }, (_, i) => (
                                  <SelectItem key={i} value={i.toString()}>
                                    {i.toString().padStart(2, '0')}
                                  </SelectItem>
                                ))}
                              </SelectContent>
                            </Select>
                          </div>
                          <div>
                            <Label htmlFor="openMinute">Minutos</Label>
                            <Select
                              value={config.openMinute.toString()}
                              onValueChange={(value) =>
                                setConfig(prev => ({ ...prev, openMinute: parseInt(value) }))
                              }
                            >
                              <SelectTrigger>
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent>
                                {[0, 15, 30, 45].map(minute => (
                                  <SelectItem key={minute} value={minute.toString()}>
                                    {minute.toString().padStart(2, '0')}
                                  </SelectItem>
                                ))}
                              </SelectContent>
                            </Select>
                          </div>
                        </div>
                      )}
                    </div>

                    <Separator />

                    {/* Auto Close Configuration */}
                    <div className="space-y-4">
                      <div className="flex items-center justify-between">
                        <div>
                          <h3 className="text-lg font-medium">Cierre Automático</h3>
                          <p className="text-sm text-gray-600">Cerrar caja y generar reporte automáticamente</p>
                        </div>
                        <Switch
                          checked={config.autoCloseEnabled}
                          onCheckedChange={(checked) =>
                            setConfig(prev => ({ ...prev, autoCloseEnabled: checked }))
                          }
                        />
                      </div>

                      {config.autoCloseEnabled && (
                        <div className="grid grid-cols-2 gap-4 ml-4 p-4 bg-gray-50 rounded-lg">
                          <div>
                            <Label htmlFor="closeHour">Hora</Label>
                            <Select
                              value={config.closeHour.toString()}
                              onValueChange={(value) =>
                                setConfig(prev => ({ ...prev, closeHour: parseInt(value) }))
                              }
                            >
                              <SelectTrigger>
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent>
                                {Array.from({ length: 24 }, (_, i) => (
                                  <SelectItem key={i} value={i.toString()}>
                                    {i.toString().padStart(2, '0')}
                                  </SelectItem>
                                ))}
                              </SelectContent>
                            </Select>
                          </div>
                          <div>
                            <Label htmlFor="closeMinute">Minutos</Label>
                            <Select
                              value={config.closeMinute.toString()}
                              onValueChange={(value) =>
                                setConfig(prev => ({ ...prev, closeMinute: parseInt(value) }))
                              }
                            >
                              <SelectTrigger>
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent>
                                {[0, 15, 30, 45].map(minute => (
                                  <SelectItem key={minute} value={minute.toString()}>
                                    {minute.toString().padStart(2, '0')}
                                  </SelectItem>
                                ))}
                              </SelectContent>
                            </Select>
                          </div>
                        </div>
                      )}
                    </div>

                    <Separator />

                    {/* Active Days */}
                    <div className="space-y-4">
                      <div>
                        <h3 className="text-lg font-medium">Días Activos</h3>
                        <p className="text-sm text-gray-600">Seleccione los días de la semana</p>
                      </div>

                      <div className="flex flex-wrap gap-2">
                        {['1', '2', '3', '4', '5', '6', '7'].map(day => {
                          const isActive = config.activeDays.split(',').includes(day);
                          return (
                            <Button
                              key={day}
                              variant={isActive ? "default" : "outline"}
                              size="sm"
                              onClick={() => {
                                const days = config.activeDays.split(',');
                                const newDays = isActive
                                  ? days.filter(d => d !== day)
                                  : [...days, day];
                                setConfig(prev => ({
                                  ...prev,
                                  activeDays: newDays.sort().join(',')
                                }));
                              }}
                            >
                              {getDayName(day)}
                            </Button>
                          );
                        })}
                      </div>
                    </div>

                    <Separator />

                    {/* Save Button */}
                    <Button
                      onClick={handleSaveConfig}
                      className="w-full"
                      disabled={updateConfigMutation.isPending}
                    >
                      {updateConfigMutation.isPending ? (
                        <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                      ) : (
                        <Save className="mr-2 h-4 w-4" />
                      )}
                      Guardar Configuración
                    </Button>
                  </CardContent>
                </Card>
              </div>

              {/* Status and Operations Panel */}
              <div className="space-y-6">

                {/* Current Schedule */}
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center">
                      <Calendar className="mr-2 h-5 w-5" />
                      Horario Actual
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-4">
                      <div className="flex justify-between items-center">
                        <span className="font-medium">Apertura:</span>
                        <div className="flex items-center space-x-2">
                          <span className="text-lg font-mono">
                            {formatTime(config.openHour, config.openMinute)}
                          </span>
                          {config.autoOpenEnabled ? (
                            <Badge variant="default">Activo</Badge>
                          ) : (
                            <Badge variant="secondary">Deshabilitado</Badge>
                          )}
                        </div>
                      </div>

                      <div className="flex justify-between items-center">
                        <span className="font-medium">Cierre:</span>
                        <div className="flex items-center space-x-2">
                          <span className="text-lg font-mono">
                            {formatTime(config.closeHour, config.closeMinute)}
                          </span>
                          {config.autoCloseEnabled ? (
                            <Badge variant="default">Activo</Badge>
                          ) : (
                            <Badge variant="secondary">Deshabilitado</Badge>
                          )}
                        </div>
                      </div>

                      <div className="flex justify-between items-center">
                        <span className="font-medium">Días activos:</span>
                        <div className="flex flex-wrap gap-1">
                          {config.activeDays.split(',').map(day => (
                            <Badge key={day} variant="outline" className="text-xs">
                              {getDayName(day)}
                            </Badge>
                          ))}
                        </div>
                      </div>
                    </div>
                  </CardContent>
                </Card>

                {/* Next Operations - USAR EXACTAMENTE LOS MISMOS DATOS QUE HORARIO ACTUAL */}
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      <Clock className="h-5 w-5" />
                      Próximas Operaciones
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    {scheduledOperations && scheduledOperations.length > 0 ? (
                      <div className="space-y-4">
                        {scheduledOperations.map((operation, index) => {
                          // Determine execution status display
                          const getExecutionStatusDisplay = (executionStatus) => {
                            switch(executionStatus) {
                              case 'executed':
                                return {
                                  badge: 'Ejecutado',
                                  badgeVariant: 'default',
                                  icon: <CheckCircle className="h-4 w-4 text-green-500" />,
                                  text: 'Ejecutado hoy',
                                  textColor: 'text-green-600 dark:text-green-400'
                                };
                              case 'next_operation':
                                return {
                                  badge: 'Próxima Operación',
                                  badgeVariant: 'outline',
                                  icon: <Clock className="h-4 w-4 text-orange-500" />,
                                  text: 'Próxima operación',
                                  textColor: 'text-orange-600 dark:text-orange-400'
                                };
                              default:
                                return {
                                  badge: 'Programado',
                                  badgeVariant: 'secondary',
                                  icon: <Clock className="h-4 w-4 text-blue-500" />,
                                  text: 'Programado hoy',
                                  textColor: 'text-blue-600 dark:text-blue-400'
                                };
                            }
                          };

                          const statusDisplay = getExecutionStatusDisplay(operation.executionStatus);

                          return (
                            <div key={index} className="flex items-center justify-between p-4 bg-gray-50 dark:bg-gray-800 rounded-lg">
                              <div className="flex items-center space-x-3">
                                <div className="text-2xl">
                                  {operation.type === 'auto_open' ? '🌅' : '🌆'}
                                </div>
                                <div>
                                  <h4 className="font-medium">
                                    {operation.type === 'auto_open' ? 'Apertura Automática' : 'Cierre Automático'}
                                  </h4>
                                  <p className="text-sm text-gray-600 dark:text-gray-400">
                                    Configurado para las {new Date(operation.scheduledTime).toLocaleTimeString('es-AR', {
                                      hour: '2-digit',
                                      minute: '2-digit',
                                      timeZone: 'America/Argentina/Buenos_Aires'
                                    })} (Argentina)
                                  </p>
                                </div>
                              </div>
                              <div className="text-right">
                                <div className="flex items-center space-x-2">
                                  {statusDisplay.icon}
                                  <span className={`text-sm ${statusDisplay.textColor}`}>
                                    {statusDisplay.text}
                                  </span>
                                </div>
                                <Badge variant={statusDisplay.badgeVariant} className="mt-1">
                                  {statusDisplay.badge}
                                </Badge>
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    ) : (
                      <div className="text-center py-4 text-gray-500 dark:text-gray-400">
                        <Clock className="h-8 w-8 mx-auto mb-2 opacity-50" />
                        <p>No hay operaciones programadas para hoy</p>
                      </div>
                    )}
                  </CardContent>
                </Card>

                {/* Operations Log */}
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      <Calendar className="h-5 w-5" />
                      Historial de Operaciones
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    {operationsLog && operationsLog.length > 0 ? (
                      <div className="space-y-3 max-h-96 overflow-y-auto">
                        {operationsLog.map((log) => (
                          <div key={log.id} className="flex items-center justify-between p-3 bg-gray-50 dark:bg-gray-800 rounded-lg">
                            <div className="flex items-center space-x-3">
                              <div className="text-lg">
                                {log.operationType === 'auto_open' ? '🌅' :
                                 log.operationType === 'auto_close' ? '🌆' : '📊'}
                              </div>
                              <div>
                                <p className="font-medium text-sm">
                                  {log.operationType === 'auto_open' ? 'Apertura Automática' :
                                   log.operationType === 'auto_close' ? 'Cierre Automático' : 'Operación Automática'}
                                </p>
                                <p className="text-xs text-gray-600 dark:text-gray-400">
                                  {new Date(log.executedTime).toLocaleString('es-AR', {
                                    timeZone: 'America/Argentina/Buenos_Aires',
                                    year: 'numeric',
                                    month: '2-digit',
                                    day: '2-digit',
                                    hour: '2-digit',
                                    minute: '2-digit'
                                  })}
                                </p>
                                {log.notes && (
                                  <p className="text-xs text-gray-500 dark:text-gray-500 mt-1">
                                    {log.notes}
                                  </p>
                                )}
                              </div>
                            </div>
                            <div>
                              {log.status === 'success' ? (
                                <Badge variant="default" className="bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300">
                                  <CheckCircle className="h-3 w-3 mr-1" />
                                  Exitoso
                                </Badge>
                              ) : (
                                <Badge variant="destructive">
                                  <XCircle className="h-3 w-3 mr-1" />
                                  Error
                                </Badge>
                              )}
                            </div>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div className="text-center py-8 text-gray-500 dark:text-gray-400">
                        <Calendar className="h-8 w-8 mx-auto mb-2 opacity-50" />
                        <p>No hay operaciones registradas aún</p>
                        <p className="text-sm">Las operaciones automáticas aparecerán aquí</p>
                      </div>
                    )}
                  </CardContent>
                </Card>

                {/* Important Notice */}
                <Alert>
                  <AlertCircle className="h-4 w-4" />
                  <AlertDescription>
                    <strong>Importante:</strong> Los reportes automáticos se generan al cerrar la caja
                    automáticamente e incluyen toda la información del día: ventas, gastos, comisiones
                    de vendedores y estadísticas completas.
                  </AlertDescription>
                </Alert>
              </div>
            </div>
          </div>
        </main>
      </div>
      <Footer />
    </div>
  );
}