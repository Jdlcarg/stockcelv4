import {
  clients,
  users,
  products,
  orders,
  orderItems,
  payments,
  currencyExchanges,
  cashRegister,
  cashMovements,
  expenses,
  debtPayments,
  dailyReports,
  generatedReports,
  customerDebts,
  productHistory,
  stockControlSessions,
  stockControlItems,
  configuration,
  companyConfiguration,
  vendors,
  customers,
  systemConfiguration,
  resellers,
  resellerSales,
  resellerConfiguration,
  type Client,
  type User,
  type Product,
  type Order,
  type OrderItem,
  type Payment,
  type CurrencyExchange,
  type CashRegister,
  type CashMovement,
  type Expense,
  type DebtPayment,
  type DailyReport,
  type GeneratedReport,
  type CustomerDebt,
  type ProductHistory,
  type StockControlSession,
  type StockControlItem,
  type Configuration,
  type CompanyConfiguration,
  type Vendor,
  type Customer,
  type SystemConfiguration,
  type Reseller,
  type ResellerSale,
  type ResellerConfiguration,
  insertClientSchema,
  insertUserSchema,
  insertProductSchema,
  insertOrderSchema,
  insertOrderItemSchema,
  insertPaymentSchema,
  insertCurrencyExchangeSchema,
  insertCashRegisterSchema,
  insertCashMovementSchema,
  insertExpenseSchema,
  insertDebtPaymentSchema,
  insertDailyReportSchema,
  insertGeneratedReportSchema,
  insertCustomerDebtSchema,
  insertProductHistorySchema,
  insertStockControlSessionSchema,
  insertStockControlItemSchema,
  insertConfigurationSchema,
  insertCompanyConfigurationSchema,
  insertVendorSchema,
  insertCustomerSchema,
  insertSystemConfigurationSchema,
  insertResellerSchema,
  insertResellerSaleSchema,
  insertResellerConfigurationSchema,
  passwordResetTokens,
  insertPasswordResetTokenSchema,
} from "@shared/schema";
import { drizzle } from "drizzle-orm/node-postgres";
import { Pool } from "pg";
import bcrypt from "bcryptjs";
import { eq, and, gte, lte, lt, like, ilike, or, desc, inArray, sum, count, not } from "drizzle-orm";
import { sql as sqlOperator } from "drizzle-orm";

type InsertClient = typeof insertClientSchema._type;
type InsertUser = typeof insertUserSchema._type;
type InsertProduct = typeof insertProductSchema._type;
type InsertOrder = typeof insertOrderSchema._type;
type InsertOrderItem = typeof insertOrderItemSchema._type;
type InsertPayment = typeof insertPaymentSchema._type;
type InsertCurrencyExchange = typeof insertCurrencyExchangeSchema._type;
type InsertCashRegister = typeof insertCashRegisterSchema._type;
type InsertCashMovement = typeof insertCashMovementSchema._type;
type InsertExpense = typeof insertExpenseSchema._type;
type InsertDebtPayment = typeof insertDebtPaymentSchema._type;
type InsertDailyReport = typeof insertDailyReportSchema._type;
type InsertGeneratedReport = typeof insertGeneratedReportSchema._type;
type InsertCustomerDebt = typeof insertCustomerDebtSchema._type;
type InsertProductHistory = typeof insertProductHistorySchema._type;
type InsertStockControlSession = typeof insertStockControlSessionSchema._type;
type InsertStockControlItem = typeof insertStockControlItemSchema._type;
type InsertConfiguration = typeof insertConfigurationSchema._type;
type InsertCompanyConfiguration = typeof insertCompanyConfigurationSchema._type;
type InsertVendor = typeof insertVendorSchema._type;
type InsertCustomer = typeof insertCustomerSchema._type;
type InsertReseller = typeof insertResellerSchema._type;
type InsertResellerSale = typeof insertResellerSaleSchema._type;
type InsertResellerConfiguration = typeof insertResellerConfigurationSchema._type;
type InsertPasswordResetToken = typeof insertPasswordResetTokenSchema._type;

export interface IStorage {
  // Clients
  createClient(client: InsertClient): Promise<Client>;
  getClientById(id: number): Promise<Client | undefined>;
  getAllClients(): Promise<Client[]>;
  getAllClientsWithAdmins(): Promise<any[]>;
  updateClient(id: number, client: Partial<InsertClient>): Promise<Client | undefined>;
  deleteClient(id: number): Promise<boolean>;
  getClientMovementsSummary(clientId: number): Promise<any>;

  // Users
  createUser(user: InsertUser): Promise<User>;
  getUserById(id: number): Promise<User | undefined>;
  getUserByEmail(email: string): Promise<User | undefined>;
  getUserByUsername(username: string): Promise<User | undefined>;
  getUsersByClientId(clientId: number): Promise<User[]>;
  updateUser(id: number, user: Partial<InsertUser>): Promise<User | undefined>;
  deleteUser(id: number): Promise<boolean>;

  // Products
  createProduct(product: InsertProduct): Promise<Product>;
  getProductById(id: number): Promise<Product | undefined>;
  getProductsByClientId(clientId: number): Promise<Product[]>;
  getProductByImei(imei: string, clientId: number): Promise<Product | undefined>;
  updateProduct(id: number, product: Partial<InsertProduct>, userId?: number): Promise<Product | undefined>;
  updateProductsByImeis(imeis: string[], clientId: number, updates: Partial<InsertProduct>, userId: number): Promise<any>;
  deleteProduct(id: number): Promise<boolean>;
  searchProducts(clientId: number, filters: {
    search?: string;
    status?: string;
    storage?: string;
    model?: string;
    quality?: string;
    battery?: string;
    provider?: string;
    priceMin?: number;
    priceMax?: number;
  }): Promise<Product[]>;

  // Product History
  createProductHistory(history: InsertProductHistory): Promise<ProductHistory>;
  getProductHistoryByProductId(productId: number): Promise<ProductHistory[]>;
  getProductHistoryByClientId(clientId: number): Promise<ProductHistory[]>;
  getProductsWithAlerts(clientId: number): Promise<any[]>;

  // Orders
  createOrder(order: InsertOrder): Promise<Order>;
  getOrderById(id: number): Promise<Order | undefined>;
  getOrdersByClientId(clientId: number): Promise<Order[]>;
  getOrdersByClientIdWithVendor(clientId: number): Promise<any[]>;
  getOrdersWithItemsAndProducts(clientId: number, vendorId?: number | null): Promise<any[]>;
  getOrdersByDateRange(clientId: number, startDate: Date, endDate: Date): Promise<Order[]>;
  updateOrder(id: number, order: Partial<InsertOrder>): Promise<Order | undefined>;
  deleteOrder(id: number): Promise<boolean>;

  // Order Items
  createOrderItem(orderItem: InsertOrderItem): Promise<OrderItem>;
  getOrderItemsByOrderId(orderId: number): Promise<OrderItem[]>;
  getOrderItemsWithProductsByOrderId(orderId: number): Promise<any[]>;
  deleteOrderItem(id: number): Promise<boolean>;

  // Payments
  createPayment(payment: InsertPayment): Promise<Payment>;
  getPaymentsByOrderId(orderId: number): Promise<Payment[]>;
  getPaymentsByClientId(clientId: number): Promise<Payment[]>;

  // Currency Exchanges
  createCurrencyExchange(exchange: InsertCurrencyExchange): Promise<CurrencyExchange>;
  getCurrencyExchangesByClientId(clientId: number): Promise<CurrencyExchange[]>;
  getCurrencyExchangesByDateRange(clientId: number, startDate: Date, endDate: Date): Promise<CurrencyExchange[]>;

  // Cash Register - Enhanced
  createCashRegister(cashRegister: InsertCashRegister): Promise<CashRegister>;
  getCurrentCashRegister(clientId: number): Promise<CashRegister | undefined>;
  getCashRegisterByDate(clientId: number, dateStr: string): Promise<CashRegister | undefined>;
  updateCashRegister(id: number, cashRegister: Partial<InsertCashRegister>): Promise<CashRegister | undefined>;
  getDailySales(clientId: number, startDate: Date, endDate: Date): Promise<number>;
  getOrCreateTodayCashRegister(clientId: number): Promise<CashRegister>;
  autoCloseCashRegister(clientId: number): Promise<CashRegister | undefined>;

  // Cash Movements
  createCashMovement(movement: InsertCashMovement): Promise<CashMovement>;
  getCashMovementsByClientId(clientId: number): Promise<CashMovement[]>;
  getCashMovementsByDateRange(clientId: number, startDate: Date, endDate: Date): Promise<CashMovement[]>;
  getCashMovementsByType(clientId: number, type: string): Promise<CashMovement[]>;
  getCashMovementsWithFilters(clientId: number, filters: {
    type?: string;
    dateFrom?: Date;
    dateTo?: Date;
    customer?: string;
    vendor?: string;
    search?: string;
    paymentMethod?: string;
  }): Promise<CashMovement[]>;
  getAllCashMovementsForExport(clientId: number): Promise<CashMovement[]>;

  // Expenses
  createExpense(expense: InsertExpense): Promise<Expense>;
  getExpensesByClientId(clientId: number): Promise<Expense[]>;
  getExpensesByDateRange(clientId: number, startDate: Date, endDate: Date): Promise<Expense[]>;
  getExpensesByCategory(clientId: number, category: string): Promise<Expense[]>;
  updateExpense(id: number, expense: Partial<InsertExpense>): Promise<Expense | undefined>;
  deleteExpense(id: number): Promise<boolean>;

  // Customer Debts
  createCustomerDebt(debt: InsertCustomerDebt): Promise<CustomerDebt>;
  getCustomerDebtsByClientId(clientId: number): Promise<CustomerDebt[]>;
  getCustomerDebtsByCustomerId(customerId: number): Promise<CustomerDebt[]>;
  getActiveDebts(clientId: number): Promise<CustomerDebt[]>;
  updateCustomerDebt(id: number, debt: Partial<InsertCustomerDebt>): Promise<CustomerDebt | undefined>;

  // Debt Payments
  createDebtPayment(payment: InsertDebtPayment): Promise<DebtPayment>;
  getDebtPaymentsByDebtId(debtId: number): Promise<DebtPayment[]>;
  getDebtPaymentsByClientId(clientId: number): Promise<DebtPayment[]>;

  // Daily Reports
  createDailyReport(report: InsertDailyReport): Promise<DailyReport>;
  getDailyReportsByClientId(clientId: number): Promise<DailyReport[]>;
  getDailyReportByDate(clientId: number, date: Date): Promise<DailyReport | undefined>;
  generateAutoDailyReport(clientId: number, date: Date): Promise<DailyReport>;

  // Real-time Cash State
  getRealTimeCashState(clientId: number): Promise<any>;
  getTotalDebtsAmount(clientId: number): Promise<number>;
  getTotalPendingVendorPayments(clientId: number): Promise<number>;
  getStockValue(clientId: number): Promise<{usd: number, ars: number}>;

  // Configuration
  createConfiguration(config: InsertConfiguration): Promise<Configuration>;
  getConfigurationByKey(clientId: number, key: string): Promise<Configuration | undefined>;
  getConfigurationsByClientId(clientId: number): Promise<Configuration[]>;
  updateConfiguration(clientId: number, key: string, value: string): Promise<Configuration>;

  // Company Configuration
  createCompanyConfiguration(config: InsertCompanyConfiguration): Promise<CompanyConfiguration>;
  getCompanyConfigurationByClientId(clientId: number): Promise<CompanyConfiguration | undefined>;
  updateCompanyConfiguration(id: number, config: Partial<InsertCompanyConfiguration>): Promise<CompanyConfiguration | undefined>;

  // Vendors
  createVendor(vendor: InsertVendor): Promise<Vendor>;
  getVendorById(id: number): Promise<Vendor | undefined>;
  getVendorsByClientId(clientId: number): Promise<Vendor[]>;
  updateVendor(id: number, vendor: Partial<InsertVendor>): Promise<Vendor | undefined>;
  deleteVendor(id: number): Promise<boolean>;

  // Customers
  createCustomer(customer: InsertCustomer): Promise<Customer>;
  getCustomerById(id: number): Promise<Customer | undefined>;
  getCustomersByClientId(clientId: number): Promise<Customer[]>;
  updateCustomer(id: number, customer: Partial<InsertCustomer>): Promise<Customer | undefined>;
  deleteCustomer(id: number): Promise<boolean>;

  // Stock Control
  createStockControlSession(session: InsertStockControlSession): Promise<StockControlSession>;
  getStockControlSessionById(id: number): Promise<StockControlSession | undefined>;
  getActiveStockControlSession(clientId: number): Promise<StockControlSession | undefined>;
  getStockControlSessionsByClientId(clientId: number): Promise<StockControlSession[]>;
  updateStockControlSession(id: number, session: Partial<InsertStockControlSession>): Promise<StockControlSession | undefined>;

  createStockControlItem(item: InsertStockControlItem): Promise<StockControlItem>;
  getStockControlItemsBySessionId(sessionId: number): Promise<StockControlItem[]>;
  getStockControlItemsWithProductsBySessionId(sessionId: number): Promise<any[]>;
  updateStockControlItem(id: number, item: Partial<InsertStockControlItem>): Promise<StockControlItem | undefined>;

  getProductsForStockControl(clientId: number): Promise<Product[]>;
  getMissingProductsFromSession(sessionId: number): Promise<Product[]>;
  getExtraviosProducts(clientId: number): Promise<Product[]>;

  // System Configuration
  getSystemConfiguration(): Promise<SystemConfiguration | undefined>;
  updateSystemConfiguration(config: Partial<SystemConfiguration>): Promise<SystemConfiguration | undefined>;

  // Resellers
  createReseller(reseller: InsertReseller): Promise<Reseller>;
  getResellers(): Promise<Reseller[]>;
  getResellerById(id: number): Promise<Reseller | undefined>;
  getResellerByEmail(email: string): Promise<Reseller | undefined>;
  updateReseller(id: number, reseller: Partial<InsertReseller>): Promise<Reseller | undefined>;
  deleteReseller(id: number): Promise<boolean>;

  // Reseller Sales
  createResellerSale(resellerId: number, saleData: any): Promise<ResellerSale>;
  getResellerSales(): Promise<ResellerSale[]>;
  getResellerSalesByReseller(resellerId: number): Promise<ResellerSale[]>;
  getClientsByResellerId(resellerId: number): Promise<any[]>;
  getResellerStats(resellerId: number): Promise<any>;

  // Reseller Configuration
  getResellerConfiguration(resellerId: number): Promise<ResellerConfiguration | undefined>;
  updateResellerConfiguration(resellerId: number, config: Partial<InsertResellerConfiguration>): Promise<ResellerConfiguration | undefined>;

  // Password Reset Tokens
  createPasswordResetToken(token: InsertPasswordResetToken): Promise<any>;
  getPasswordResetToken(token: string): Promise<any>;
  markPasswordResetTokenAsUsed(id: number): Promise<boolean>;
}

export class MemStorage implements IStorage {
  private clients: Map<number, Client> = new Map();
  private users: Map<number, User> = new Map();
  private products: Map<number, Product> = new Map();
  private orders: Map<number, Order> = new Map();
  private orderItems: Map<number, OrderItem> = new Map();
  private payments: Map<number, Payment> = new Map();
  private currencyExchanges: Map<number, CurrencyExchange> = new Map();
  private cashRegisters: Map<number, CashRegister> = new Map();
  private productHistories: Map<number, ProductHistory> = new Map();
  private configurations: Map<number, Configuration> = new Map();
  private companyConfigurations: Map<number, CompanyConfiguration> = new Map();
  private vendors: Map<number, Vendor> = new Map();
  private customers: Map<number, Customer> = new Map();
  private stockControlSessions: Map<number, StockControlSession> = new Map();
  private stockControlItems: Map<number, StockControlItem> = new Map();

  private currentClientId = 1;
  private currentUserId = 5;
  private currentProductId = 1;
  private currentOrderId = 1;
  private currentOrderItemId = 1;
  private currentPaymentId = 1;
  private currentCurrencyExchangeId = 1;
  private currentCashRegisterId = 1;
  private currentProductHistoryId = 1;
  private currentConfigurationId = 1;
  private currentCompanyConfigurationId = 1;
  private currentVendorId = 1;
  private currentCustomerId = 1;

  constructor() {
    // Create default client and superuser
    this.seedDefaultData();
  }

  private seedDefaultData() {
    // Create default client
    const defaultClient: Client = {
      id: 1,
      name: "Empresa Demo",
      email: "demo@stockcel.com",
      phone: "+54 11 1234-5678",
      address: "Buenos Aires, Argentina",
      isActive: true,
      createdAt: new Date(),
    };
    this.clients.set(1, defaultClient);

    // Create superuser
    const superuser: User = {
      id: 1,
      clientId: 1,
      username: "admin",
      email: "admin@stockcel.com",
      password: "admin123", // In production, this should be hashed
      role: "superuser",
      isActive: true,
      createdAt: new Date(),
    };
    this.users.set(1, superuser);

    // Create developer user (alternative superuser)
    const developerUser: User = {
      id: 4,
      clientId: 1,
      username: "developer",
      email: "developer@stockcel.com",
      password: "dev123",
      role: "superuser",
      isActive: true,
      createdAt: new Date(),
    };
    this.users.set(4, developerUser);

    // Create demo admin user
    const adminUser: User = {
      id: 2,
      clientId: 1,
      username: "juan.perez",
      email: "juan@demo.com",
      password: "demo123",
      role: "admin",
      isActive: true,
      createdAt: new Date(),
    };
    this.users.set(2, adminUser);

    // Create demo vendor user
    const vendorUser: User = {
      id: 3,
      clientId: 1,
      username: "maria.garcia",
      email: "maria@demo.com",
      password: "demo123",
      role: "vendor",
      isActive: true,
      createdAt: new Date(),
    };
    this.users.set(3, vendorUser);

    // Create demo vendors
    const demoVendor1: Vendor = {
      id: 1,
      clientId: 1,
      name: "Juan Pérez",
      phone: "+54 11 1234-5678",
      email: "juan@demo.com",
      commission: "5.00",
      isActive: true,
      createdAt: new Date(),
      updatedAt: new Date(),
    };
    this.vendors.set(1, demoVendor1);

    const demoVendor2: Vendor = {
      id: 2,
      clientId: 1,
      name: "María García",
      phone: "+54 11 2345-6789",
      email: "maria@demo.com",
      commission: "7.50",
      isActive: true,
      createdAt: new Date(),
      updatedAt: new Date(),
    };
    this.vendors.set(2, demoVendor2);

    // Create demo customers
    const demoCustomer1: Customer = {
      id: 1,
      clientId: 1,
      name: "Cliente Demo 1",
      phone: "+54 11 3456-7890",
      email: "cliente1@demo.com",
      address: "Av. Corrientes 1234, Buenos Aires",
      identification: "DNI 12345678",
      notes: "Cliente frecuente",
      isActive: true,
      createdAt: new Date(),
      updatedAt: new Date(),
    };
    this.customers.set(1, demoCustomer1);

    const demoCustomer2: Customer = {
      id: 2,
      clientId: 1,
      name: "Cliente Demo 2",
      phone: "+54 11 4567-8901",
      email: "cliente2@demo.com",
      address: "Av. Santa Fe 5678, Buenos Aires",
      identification: "DNI 87654321",
      notes: "Cliente corporativo",
      isActive: true,
      createdAt: new Date(),
      updatedAt: new Date(),
    };
    this.customers.set(2, demoCustomer2);

    // Create demo products
    const demoProducts: Product[] = [
      {
        id: 1,
        clientId: 1,
        imei: "123456789012345",
        model: "iPhone 14 Pro",
        storage: "256GB",
        color: "Negro",
        costPrice: "899.99",
        status: "disponible",
        proveedor: "Apple Store",
        battery: "95%",
        quality: "Excelente",
        createdAt: new Date(),
        updatedAt: new Date(),
      },
      {
        id: 2,
        clientId: 1,
        imei: "987654321098765",
        model: "Samsung Galaxy S23",
        storage: "128GB",
        color: "Blanco",
        costPrice: "749.99",
        status: "disponible",
        proveedor: "Samsung Store",
        battery: "98%",
        quality: "Nuevo",
        createdAt: new Date(),
        updatedAt: new Date(),
      },
      {
        id: 3,
        clientId: 1,
        imei: "456789012345678",
        model: "iPhone 13",
        storage: "128GB",
        color: "Azul",
        costPrice: "649.99",
        status: "disponible",
        proveedor: "Apple Store",
        battery: "92%",
        quality: "Excelente",
        createdAt: new Date(),
        updatedAt: new Date(),
      },
      {
        id: 4,
        clientId: 1,
        imei: "789012345678901",
        model: "Google Pixel 8",
        storage: "256GB",
        color: "Gris",
        costPrice: "699.99",
        status: "disponible",
        proveedor: "Google Store",
        battery: "100%",
        quality: "Nuevo",
        createdAt: new Date(),
        updatedAt: new Date(),
      },
      {
        id: 5,
        clientId: 1,
        imei: "234567890123456",
        model: "Xiaomi Mi 13",
        storage: "256GB",
        color: "Verde",
        costPrice: "549.99",
        status: "disponible",
        proveedor: "Xiaomi Store",
        battery: "96%",
        quality: "Excelente",
        createdAt: new Date(),
        updatedAt: new Date(),
      }
    ];

    demoProducts.forEach(product => {
      this.products.set(product.id, product);
    });

    // Initialize counters
    this.currentClientId = 2;
    this.currentUserId = 4;
    this.currentVendorId = 3;
    this.currentCustomerId = 3;
    this.currentProductId = 6;
  }

  // Client methods
  async createClient(client: InsertClient): Promise<Client> {
    const newClient: Client = {
      ...client,
      id: this.currentClientId++,
      createdAt: new Date(),
    };
    this.clients.set(newClient.id, newClient);
    return newClient;
  }

  async getClientById(id: number): Promise<Client | undefined> {
    return this.clients.get(id);
  }

  async getAllClients(): Promise<Client[]> {
    return Array.from(this.clients.values());
  }

  async getAllClientsWithAdmins(): Promise<any[]> {
    const allClients = Array.from(this.clients.values());
    const result = [];

    for (const client of allClients) {
      // Get admin user for this client
      const adminUser = Array.from(this.users.values()).find(
        user => user.clientId === client.id && user.role === 'admin'
      );

      result.push({
        ...client,
        adminUsername: adminUser?.username || 'No admin',
        adminName: adminUser?.username || 'No admin',
        adminEmail: adminUser?.email || '',
      });
    }

    return result;
  }

  async updateClient(id: number, client: Partial<InsertClient>): Promise<Client | undefined> {
    const existing = this.clients.get(id);
    if (!existing) return undefined;

    const updated: Client = { ...existing, ...client };
    this.clients.set(id, updated);
    return updated;
  }

  async deleteClient(id: number): Promise<boolean> {
    return this.clients.delete(id);
  }

  // User methods
  async createUser(user: InsertUser): Promise<User> {
    const newUser: User = {
      ...user,
      id: this.currentUserId++,
      createdAt: new Date(),
    };
    this.users.set(newUser.id, newUser);
    return newUser;
  }

  async getUserById(id: number): Promise<User | undefined> {
    return this.users.get(id);
  }

  async getUserByEmail(email: string): Promise<User | undefined> {
    return Array.from(this.users.values()).find(user => user.email === email);
  }

  async getUsersByClientIdAndRole(clientId: number, role: string): Promise<User[]> {
    return Array.from(this.users.values()).filter(user =>
      user.clientId === clientId && user.role === role
    );
  }

  async getUserByUsername(username: string): Promise<User | undefined> {
    return Array.from(this.users.values()).find(user => user.username === username);
  }

  async getUsersByClientId(clientId: number): Promise<User[]> {
    return Array.from(this.users.values()).filter(user => user.clientId === clientId);
  }

  async updateUser(id: number, user: Partial<InsertUser>): Promise<User | undefined> {
    const existing = this.users.get(id);
    if (!existing) return undefined;

    const updated: User = { ...existing, ...user };
    this.users.set(id, updated);
    return updated;
  }

  async deleteUser(id: number): Promise<boolean> {
    return this.users.delete(id);
  }

  // Product methods
  async createProduct(product: InsertProduct): Promise<Product> {
    const newProduct: Product = {
      ...product,
      id: this.currentProductId++,
      createdAt: new Date(),
      updatedAt: new Date(),
    };
    this.products.set(newProduct.id, newProduct);
    return newProduct;
  }

  async getProductById(id: number): Promise<Product | undefined> {
    return this.products.get(id);
  }

  async getProductsByClientId(clientId: number): Promise<Product[]> {
    return Array.from(this.products.values()).filter(product => product.clientId === clientId);
  }

  async getProductByImei(imei: string, clientId: number): Promise<Product | undefined> {
    return Array.from(this.products.values()).find(
      product => product.imei === imei && product.clientId === clientId
    );
  }

  async updateProduct(id: number, product: Partial<InsertProduct>, userId?: number): Promise<Product | undefined> {
    const existing = this.products.get(id);
    if (!existing) return undefined;

    const updated: Product = { ...existing, ...product, updatedAt: new Date() };
    this.products.set(id, updated);

    // Create history record for ALL changes (not just status)
    if (userId) {
      const changes = [];
      const fieldNames = {
        'status': 'Estado',
        'costPrice': 'Precio Costo',
        'quality': 'Calidad',
        'battery': 'Batería',
        'provider': 'Proveedor',
        'observations': 'Observaciones',
        'model': 'Modelo',
        'storage': 'Almacenamiento',
        'color': 'Color',
        'imei': 'IMEI'
      };

      const fieldsToTrack = ['status', 'costPrice', 'quality', 'battery', 'provider', 'observations', 'model', 'storage', 'color', 'imei'];

      for (const field of fieldsToTrack) {
        if (product[field as keyof typeof product] !== undefined && product[field as keyof typeof product] !== existing[field as keyof typeof existing]) {
          const oldValue = existing[field as keyof typeof existing] || 'vacío';
          const newValue = product[field as keyof typeof product] || 'vacío';
          const fieldName = fieldNames[field as keyof typeof fieldNames] || field;
          changes.push(`${fieldName}: ${oldValue} → ${newValue}`);
        }
      }

      if (changes.length > 0) {
        await this.createProductHistory({
          clientId: existing.clientId,
          productId: id,
          previousStatus: existing.status,
          newStatus: product.status || existing.status,
          userId: userId,
          notes: `Cambios: ${changes.join(', ')}`,
        });
      }
    }

    return updated;
  }

  async deleteProduct(id: number): Promise<boolean> {
    return this.products.delete(id);
  }

  async updateProductsByImeis(imeis: string[], clientId: number, updates: Partial<InsertProduct>, userId: number): Promise<{ success: number; notFound: string[] }> {
    const notFound: string[] = [];
    let success = 0;

    for (const imei of imeis) {
      const product = await this.getProductByImei(imei.trim(), clientId);
      if (product) {
        await this.updateProduct(product.id, updates, userId);
        success++;
      } else {
        notFound.push(imei);
      }
    }

    return { success, notFound };
  }

  async searchProducts(clientId: number, filters: {
    search?: string;
    status?: string;
    storage?: string;
    model?: string;
    quality?: string;
    battery?: string;
    provider?: string;
    priceMin?: number;
    priceMax?: number;
  }): Promise<Product[]> {
    let results = Array.from(this.products.values()).filter(product => product.clientId === clientId);

    if (filters.search) {
      const searchTerm = filters.search.toLowerCase();
      results = results.filter(product =>
        product.imei.toLowerCase().includes(searchTerm) ||
        product.model.toLowerCase().includes(searchTerm) ||
        (product.provider && product.provider.toLowerCase().includes(searchTerm))
      );
    }

    if (filters.status) {
      results = results.filter(product => product.status === filters.status);
    }

    if (filters.storage) {
      results = results.filter(product => product.storage === filters.storage);
    }

    if (filters.model) {
      results = results.filter(product => product.model.toLowerCase().includes(filters.model.toLowerCase()));
    }

    if (filters.quality) {
      results = results.filter(product => product.quality === filters.quality);
    }

    if (filters.battery) {
      results = results.filter(product => product.battery === filters.battery);
    }

    if (filters.provider) {
      results = results.filter(product => product.provider && product.provider.toLowerCase().includes(filters.provider.toLowerCase()));
    }

    if (filters.priceMin !== undefined) {
      results = results.filter(product => parseFloat(product.costPrice) >= filters.priceMin!);
    }

    if (filters.priceMax !== undefined) {
      results = results.filter(product => parseFloat(product.costPrice) <= filters.priceMax!);
    }

    return results;
  }

  // Product History methods
  async createProductHistory(history: InsertProductHistory): Promise<ProductHistory> {
    const newHistory: ProductHistory = {
      ...history,
      id: this.currentProductHistoryId++,
      createdAt: new Date(),
    };
    this.productHistories.set(newHistory.id, newHistory);
    return newHistory;
  }

  async getProductHistoryByProductId(productId: number): Promise<ProductHistory[]> {
    return Array.from(this.productHistories.values())
      .filter(history => history.productId === productId)
      .sort((a, b) => {
        const dateA = a.createdAt ? new Date(a.createdAt).getTime() : 0;
        const dateB = b.createdAt ? new Date(b.createdAt).getTime() : 0;
        return dateB - dateA;
      });
  }

  async getProductHistoryByClientId(clientId: number): Promise<ProductHistory[]> {
    return Array.from(this.productHistories.values())
      .filter(history => history.clientId === clientId)
      .sort((a, b) => {
        const dateA = a.createdAt ? new Date(a.createdAt).getTime() : 0;
        const dateB = b.createdAt ? new Date(b.createdAt).getTime() : 0;
        return dateB - dateA;
      });
  }

  async getProductsWithAlerts(clientId: number): Promise<any[]> {
    const products = Array.from(this.products.values()).filter(product => product.clientId === clientId);
    const alerts = [];
    const now = new Date();

    for (const product of products) {
      const histories = await this.getProductHistoryByProductId(product.id);
      const lastStatusChange = histories.find(h => h.newStatus === product.status);

      if (lastStatusChange && lastStatusChange.createdAt) {
        const daysSinceChange = Math.floor((now.getTime() - new Date(lastStatusChange.createdAt).getTime()) / (1000 * 60 * 60 * 24));

        let shouldAlert = false;
        let alertMessage = '';

        if (product.status === 'reservado' && daysSinceChange >= 2) {
          shouldAlert = true;
          alertMessage = `Producto reservado por ${daysSinceChange} días`;
        } else if (product.status === 'tecnico_interno' && daysSinceChange >= 5) {
          shouldAlert = true;
          alertMessage = `Producto en técnico interno por ${daysSinceChange} días`;
        } else if (product.status === 'tecnico_externo' && daysSinceChange >= 7) {
          shouldAlert = true;
          alertMessage = `Producto en técnico externo por ${daysSinceChange} días`;
        }

        if (shouldAlert) {
          alerts.push({
            product,
            daysSinceChange,
            alertMessage,
            lastStatusChange: lastStatusChange.createdAt
          });
        }
      }
    }

    return alerts;
  }

  // Order methods
  async createOrder(order: InsertOrder): Promise<Order> {
    const newOrder: Order = {
      ...order,
      id: this.currentOrderId++,
      createdAt: new Date(),
      updatedAt: new Date(),
    };
    this.orders.set(newOrder.id, newOrder);
    return newOrder;
  }

  async getOrderById(id: number): Promise<Order | undefined> {
    return this.orders.get(id);
  }

  async getOrdersByClientId(clientId: number): Promise<Order[]> {
    return Array.from(this.orders.values())
      .filter(order => order.clientId === clientId)
      .sort((a, b) => new Date(b.createdAt || 0).getTime() - new Date(a.createdAt || 0).getTime());
  }

  async getOrdersByClientIdWithVendor(clientId: number): Promise<any[]> {
    const clientOrders = Array.from(this.orders.values())
      .filter(order => order.clientId === clientId)
      .sort((a, b) => new Date(b.createdAt || 0).getTime() - new Date(a.createdAt || 0).getTime());

    const result = [];
    for (const order of clientOrders) {
      const vendor = order.vendorId ? this.vendors.get(order.vendorId) : null;
      result.push({
        ...order,
        vendorName: vendor?.name || 'Sin asignar'
      });
    }

    return result;
  }

  async getOrdersWithItemsAndProducts(clientId: number, vendorId?: number | null): Promise<any[]> {
    let clientOrders = Array.from(this.orders.values())
      .filter(order => order.clientId === clientId);

    if (vendorId) {
      clientOrders = clientOrders.filter(order => order.vendorId === vendorId);
    }

    clientOrders.sort((a, b) => new Date(b.createdAt || 0).getTime() - new Date(a.createdAt || 0).getTime());

    const result = [];
    for (const order of clientOrders) {
      const vendor = order.vendorId ? this.vendors.get(order.vendorId) : null;
      const orderItems = Array.from(this.orderItems.values()).filter(item => item.orderId === order.id);

      const itemsWithProducts = [];
      for (const item of orderItems) {
        const product = this.products.get(item.productId);
        itemsWithProducts.push({
          ...item,
          product
        });
      }

      result.push({
        ...order,
        vendorName: vendor?.name || 'Sin asignar',
        items: itemsWithProducts
      });
    }

    return result;
  }

  async getOrdersByDateRange(clientId: number, startDate: Date, endDate: Date): Promise<Order[]> {
    return Array.from(this.orders.values())
      .filter(order =>
        order.clientId === clientId &&
        order.createdAt &&
        new Date(order.createdAt) >= startDate &&
        new Date(order.createdAt) <= endDate
      )
      .sort((a, b) => new Date(b.createdAt || 0).getTime() - new Date(a.createdAt || 0).getTime());
  }

  async updateOrder(id: number, order: Partial<InsertOrder>): Promise<Order | undefined> {
    const existing = this.orders.get(id);
    if (!existing) return undefined;

    const updated: Order = { ...existing, ...order, updatedAt: new Date() };
    this.orders.set(id, updated);
    return updated;
  }

  async deleteOrder(id: number): Promise<boolean> {
    return this.orders.delete(id);
  }

  // Order Item methods
  async createOrderItem(orderItem: InsertOrderItem): Promise<OrderItem> {
    const newOrderItem: OrderItem = {
      ...orderItem,
      id: this.currentOrderItemId++,
      createdAt: new Date(),
    };
    this.orderItems.set(newOrderItem.id, newOrderItem);
    return newOrderItem;
  }

  async getOrderItemsByOrderId(orderId: number): Promise<OrderItem[]> {
    console.log('MemStorage method called, not DrizzleStorage');
    return Array.from(this.orderItems.values()).filter(item => item.orderId === orderId);
  }

  async getOrderItemsWithProductsByOrderId(orderId: number): Promise<any[]> {
    console.log('MemStorage: Getting order items with products for order ID:', orderId);

    // Get real order items from memory storage
    const orderItems = Array.from(this.orderItems.values()).filter(item => item.orderId === orderId);
    const result = [];

    for (const item of orderItems) {
      const product = this.products.get(item.productId);
      result.push({
        ...item,
        product
      });
    }

    console.log('MemStorage: Returning order items with products:', result);
    return result;
  }

  async deleteOrderItem(id: number): Promise<boolean> {
    return this.orderItems.delete(id);
  }

  // Payment methods
  async createPayment(payment: InsertPayment): Promise<Payment> {
    const newPayment: Payment = {
      ...payment,
      id: this.currentPaymentId++,
      createdAt: new Date(),
    };
    this.payments.set(newPayment.id, newPayment);
    return newPayment;
  }

  async getPaymentsByOrderId(orderId: number): Promise<Payment[]> {
    return Array.from(this.payments.values()).filter(payment => payment.orderId === orderId);
  }

  async getPaymentsByClientId(clientId: number): Promise<Payment[]> {
    return Array.from(this.payments.values()).filter(payment => payment.clientId === clientId);
  }

  // Currency Exchange methods
  async createCurrencyExchange(exchange: InsertCurrencyExchange): Promise<CurrencyExchange> {
    const newExchange: CurrencyExchange = {
      ...exchange,
      id: this.currentCurrencyExchangeId++,
      createdAt: new Date(),
    };
    this.currencyExchanges.set(newExchange.id, newExchange);
    return newExchange;
  }

  async getCurrencyExchangesByClientId(clientId: number): Promise<CurrencyExchange[]> {
    return Array.from(this.currencyExchanges.values()).filter(exchange => exchange.clientId === clientId);
  }

  async getCurrencyExchangesByDateRange(clientId: number, startDate: Date, endDate: Date): Promise<CurrencyExchange[]> {
    return Array.from(this.currencyExchanges.values()).filter(exchange =>
      exchange.clientId === clientId &&
      exchange.createdAt >= startDate &&
      exchange.createdAt <= endDate
    );
  }

  // Cash Register methods
  async createCashRegister(cashRegister: InsertCashRegister): Promise<CashRegister> {
    const newCashRegister: CashRegister = {
      ...cashRegister,
      id: this.currentCashRegisterId++,
      createdAt: new Date(),
    };
    this.cashRegisters.set(newCashRegister.id, newCashRegister);
    return newCashRegister;
  }

  async getCurrentCashRegister(clientId: number): Promise<CashRegister | undefined> {
    console.log(`🔍 [STORAGE] getCurrentCashRegister called for clientId: ${clientId}`);
    console.log(`🔍 [STORAGE] Total cash registers in memory: ${this.cashRegisters.size}`);

    const allRegisters = Array.from(this.cashRegisters.values());
    console.log(`🔍 [STORAGE] All cash registers:`, allRegisters.map(r => ({
      id: r.id,
      clientId: r.clientId,
      isOpen: r.isOpen,
      date: r.date
    })));

    const clientRegisters = allRegisters.filter(register => register.clientId === clientId);
    console.log(`🔍 [STORAGE] Registers for client ${clientId}:`, clientRegisters.map(r => ({
      id: r.id,
      isOpen: r.isOpen,
      date: r.date
    })));

    const openRegister = clientRegisters.find(register => register.isOpen);
    console.log(`🔍 [STORAGE] Open register for client ${clientId}:`, openRegister ? {
      id: openRegister.id,
      isOpen: openRegister.isOpen,
      date: openRegister.date
    } : null);

    return openRegister;
  }

  async getCashRegisterByDate(clientId: number, dateStr: string): Promise<CashRegister | undefined> {
    const targetDate = new Date(dateStr);
    const startOfDay = new Date(targetDate);
    startOfDay.setHours(0, 0, 0, 0);

    const endOfDay = new Date(targetDate);
    endOfDay.setHours(23, 59, 59, 999);

    return Array.from(this.cashRegisters.values()).find(
      register =>
        register.clientId === clientId &&
        register.createdAt >= startOfDay &&
        register.createdAt <= endOfDay
    );
  }

  async updateCashRegister(id: number, cashRegister: Partial<InsertCashRegister>): Promise<CashRegister | undefined> {
    const existing = this.cashRegisters.get(id);
    if (!existing) return undefined;

    const updated: CashRegister = { ...existing, ...cashRegister };
    this.cashRegisters.set(id, updated);
    return updated;
  }

  // Configuration methods
  async createConfiguration(config: InsertConfiguration): Promise<Configuration> {
    const newConfig: Configuration = {
      ...config,
      id: this.currentConfigurationId++,
      createdAt: new Date(),
      updatedAt: new Date(),
    };
    this.configurations.set(newConfig.id, newConfig);
    return newConfig;
  }

  async getConfigurationByKey(clientId: number, key: string): Promise<Configuration | undefined> {
    return Array.from(this.configurations.values()).find(
      config => config.clientId === clientId && config.key === key
    );
  }

  async getConfigurationsByClientId(clientId: number): Promise<Configuration[]> {
    return Array.from(this.configurations.values()).filter(
      config => config.clientId === clientId
    );
  }

  async updateConfiguration(clientId: number, key: string, value: string): Promise<Configuration> {
    // First try to find existing configuration
    const existing = await this.getConfigurationByKey(clientId, key);

    if (existing) {
      // Update existing configuration
      const updated: Configuration = {
        ...existing,
        value,
        updatedAt: new Date()
      };
      this.configurations.set(existing.id, updated);
      return updated;
    } else {
      // Create new configuration
      return await this.createConfiguration({
        clientId,
        key,
        value
      });
    }
  }

  // Company Configuration methods
  async createCompanyConfiguration(config: InsertCompanyConfiguration): Promise<CompanyConfiguration> {
    const newConfig: CompanyConfiguration = {
      ...config,
      id: this.currentCompanyConfigurationId++,
      createdAt: new Date(),
      updatedAt: new Date(),
    };
    this.companyConfigurations.set(newConfig.id, newConfig);
    return newConfig;
  }

  async getCompanyConfigurationByClientId(clientId: number): Promise<CompanyConfiguration | undefined> {
    return Array.from(this.companyConfigurations.values()).find(
      config => config.clientId === clientId
    );
  }

  async updateCompanyConfiguration(id: number, config: Partial<InsertCompanyConfiguration>): Promise<CompanyConfiguration | undefined> {
    const existing = this.companyConfigurations.get(id);
    if (!existing) return undefined;

    const updated: CompanyConfiguration = { ...existing, ...config, updatedAt: new Date() };
    this.companyConfigurations.set(id, updated);
    return updated;
  }

  // Vendor methods
  async createVendor(vendor: InsertVendor): Promise<Vendor> {
    const newVendor: Vendor = {
      ...vendor,
      id: this.currentVendorId++,
      createdAt: new Date(),
      updatedAt: new Date(),
    };
    this.vendors.set(newVendor.id, newVendor);
    return newVendor;
  }

  async getVendorById(id: number): Promise<Vendor | undefined> {
    return this.vendors.get(id);
  }

  async getVendorsByClientId(clientId: number): Promise<Vendor[]> {
    return Array.from(this.vendors.values()).filter(vendor => vendor.clientId === clientId);
  }

  async updateVendor(id: number, vendor: Partial<InsertVendor>): Promise<Vendor | undefined> {
    const existing = this.vendors.get(id);
    if (!existing) return undefined;

    const updated: Vendor = { ...existing, ...vendor, updatedAt: new Date() };
    this.vendors.set(id, updated);
    return updated;
  }

  async deleteVendor(id: number): Promise<boolean> {
    return this.vendors.delete(id);
  }

  // Customer methods
  async createCustomer(customer: InsertCustomer): Promise<Customer> {
    const newCustomer: Customer = {
      ...customer,
      id: this.currentCustomerId++,
      createdAt: new Date(),
      updatedAt: new Date(),
    };
    this.customers.set(newCustomer.id, newCustomer);
    return newCustomer;
  }

  async getCustomerById(id: number): Promise<Customer | undefined> {
    return this.customers.get(id);
  }

  async getCustomersByClientId(clientId: number): Promise<Customer[]> {
    return Array.from(this.customers.values()).filter(customer => customer.clientId === clientId);
  }

  async updateCustomer(id: number, customer: Partial<InsertCustomer>): Promise<Customer | undefined> {
    const existing = this.customers.get(id);
    if (!existing) return undefined;

    const updated: Customer = { ...existing, ...customer, updatedAt: new Date() };
    this.customers.set(id, updated);
    return updated;
  }

  async deleteCustomer(id: number): Promise<boolean> {
    return this.customers.delete(id);
  }

  async getCurrentCashRegister(clientId: number): Promise<CashRegister | undefined> {
    console.log(`🔍 [STORAGE] getCurrentCashRegister called for clientId: ${clientId}`);
    console.log(`🔍 [STORAGE] Total cash registers in memory: ${this.cashRegisters.size}`);

    const allRegisters = Array.from(this.cashRegisters.values());
    console.log(`🔍 [STORAGE] All cash registers:`, allRegisters.map(r => ({
      id: r.id,
      clientId: r.clientId,
      isOpen: r.isOpen,
      date: r.date
    })));

    const clientRegisters = allRegisters.filter(register => register.clientId === clientId);
    console.log(`🔍 [STORAGE] Registers for client ${clientId}:`, clientRegisters.map(r => ({
      id: r.id,
      isOpen: r.isOpen,
      date: r.date
    })));

    const openRegister = clientRegisters.find(register => register.isOpen);
    console.log(`🔍 [STORAGE] Open register for client ${clientId}:`, openRegister ? {
      id: openRegister.id,
      isOpen: openRegister.isOpen,
      date: openRegister.date
    } : null);

    return openRegister;
  }

  async getDailySales(clientId: number, startDate: Date, endDate: Date): Promise<number> {
    const orders = Array.from(this.orders.values()).filter(order =>
      order.clientId === clientId &&
      order.createdAt >= startDate &&
      order.createdAt < endDate
    );

    return orders.reduce((sum, order) => sum + parseFloat(order.totalUsd), 0);
  }

  async createStockControlSession(sessionData: InsertStockControlSession): Promise<StockControlSession> {
    console.log('📊 DrizzleStorage: Creating stock control session with data:', JSON.stringify(sessionData, null, 2));

    try {
      const [result] = await db.insert(stockControlSessions).values(sessionData).returning();
      console.log('✅ DrizzleStorage: Stock control session created successfully:', JSON.stringify(result, null, 2));
      return result;
    } catch (error) {
      console.error('❌ DrizzleStorage: Error in createStockControlSession:', error);
      throw error;
    }
  }

  async getStockControlSessionById(id: number): Promise<StockControlSession | undefined> {
    const [result] = await db.select().from(stockControlSessions).where(eq(stockControlSessions.id, id));
    return result;
  }

  async getActiveStockControlSession(clientId: number): Promise<StockControlSession | undefined> {
    // Get any session that is either 'active' or 'paused' (not completed)
    const [result] = await db.select().from(stockControlSessions)
      .where(and(
        eq(stockControlSessions.clientId, clientId),
        inArray(stockControlSessions.status, ['active', 'paused'])
      ))
      .orderBy(desc(stockControlSessions.createdAt))
      .limit(1);

    console.log('🔍 getActiveStockControlSession - Found session:', result ? `ID ${result.id} Status: ${result.status}` : 'None');
    return result;
  }

  async getStockControlSessionsByClientId(clientId: number): Promise<StockControlSession[]> {
    return await db.select().from(stockControlSessions)
      .where(eq(stockControlSessions.clientId, clientId))
      .orderBy(desc(stockControlSessions.createdAt));
  }

  async updateStockControlSession(id: number, session: Partial<InsertStockControlSession>): Promise<StockControlSession | undefined> {
    const [result] = await db.update(stockControlSessions)
      .set(session)
      .where(eq(stockControlSessions.id, id))
      .returning();
    return result;
  }

  async createStockControlItem(item: InsertStockControlItem): Promise<StockControlItem> {
    const [result] = await db.insert(stockControlItems).values(item).returning();
    return result;
  }

  async getStockControlItemsBySessionId(sessionId: number): Promise<StockControlItem[]> {
    return await db.select().from(stockControlItems)
      .where(eq(stockControlItems.sessionId, sessionId));
  }

  async getStockControlItemsWithProductsBySessionId(sessionId: number): Promise<any[]> {
    console.log('🔍 Getting items for session:', sessionId);

    const result = await db.select({
      id: stockControlItems.id,
      sessionId: stockControlItems.sessionId,
      productId: stockControlItems.productId,
      imei: stockControlItems.imei,
      scannedAt: stockControlItems.scannedAt,
      status: stockControlItems.status,
      actionTaken: stockControlItems.actionTaken,
      notes: stockControlItems.notes,
      createdAt: stockControlItems.createdAt,
      // Product fields
      productImei: products.imei,
      productModel: products.model,
      productStorage: products.storage,
      productColor: products.color,
      productCostPrice: products.costPrice,
      productStatus: products.status,
      productClientId: products.clientId
    })
    .from(stockControlItems)
    .leftJoin(products, eq(stockControlItems.productId, products.id))
    .where(eq(stockControlItems.sessionId, sessionId))
    .orderBy(desc(stockControlItems.scannedAt));

    console.log('📦 Found', result.length, 'items for session', sessionId);

    const mappedItems = result.map(item => ({
      id: item.id,
      sessionId: item.sessionId,
      productId: item.productId,
      imei: item.imei,
      scannedAt: item.scannedAt,
      status: item.status,
      actionTaken: item.actionTaken,
      notes: item.notes,
      createdAt: item.createdAt,
      // Include product data directly at root level for easier access
      productModel: item.productModel,
      productStorage: item.productStorage,
      productColor: item.productColor,
      productCostPrice: item.productCostPrice,
      productStatus: item.productStatus,
      // Also include nested product object
      product: item.productImei ? {
        id: item.productId,
        imei: item.productImei,
        model: item.productModel,
        storage: item.productStorage,
        color: item.productColor,
        costPrice: item.productCostPrice,
        status: item.productStatus,
        clientId: item.productClientId
      } : null
    }));

    console.log('✅ Mapped items for session', sessionId, ':', mappedItems.length);
    return mappedItems;
  }

  async updateStockControlItem(id: number, item: Partial<InsertStockControlItem>): Promise<StockControlItem | undefined> {
    const [result] = await db.update(stockControlItems)
      .set(item)
      .where(eq(stockControlItems.id, id))
      .returning();
    return result;
  }

  async getProductsForStockControl(clientId: number): Promise<Product[]> {
    return await db.select().from(products)
      .where(and(
        eq(products.clientId, clientId),
        inArray(products.status, ['disponible', 'reservado', 'tecnico_interno'])
      ));
  }

  async getMissingProductsFromSession(sessionId: number): Promise<Product[]> {
    // Get session details
    const session = await this.getStockControlSessionById(sessionId);
    if (!session) return [];

    // Get all scanned items from this session
    const scannedItems = await this.getStockControlItemsBySessionId(sessionId);
    const scannedIMEIs = scannedItems.map(item => item.imei);

    // Get all products that should have been scanned but weren't
    const expectedProducts = await this.getProductsForStockControl(session.clientId);

    return expectedProducts.filter(product => !scannedIMEIs.includes(product.imei));
  }

  async getExtraviosProducts(clientId: number): Promise<Product[]> {
    return await db.select().from(products)
      .where(and(
        eq(products.clientId, clientId),
        eq(products.status, 'extravio')
      ));
  }

  // =======================
  // CASH MOVEMENTS - Enhanced System
  // =======================

  async createCashMovement(movement: InsertCashMovement): Promise<CashMovement> {
    const [result] = await db.insert(cashMovements).values(movement).returning();
    return result;
  }

  async getCashMovementsByClientId(clientId: number): Promise<CashMovement[]> {
    return await db
      .select({
        id: cashMovements.id,
        clientId: cashMovements.clientId,
        cashRegisterId: cashMovements.cashRegisterId,
        type: cashMovements.type,
        subtype: cashMovements.subtype,
        amount: cashMovements.amount,
        currency: cashMovements.currency,
        exchangeRate: cashMovements.exchangeRate,
        amountUsd: cashMovements.amountUsd,
        description: cashMovements.description,
        referenceId: cashMovements.referenceId,
        referenceType: cashMovements.referenceType,
        customerId: cashMovements.customerId,
        vendorId: cashMovements.vendorId,
        userId: cashMovements.userId,
        notes: cashMovements.notes,
        createdAt: cashMovements.createdAt,
        userName: users.username,
        customerName: customers.name
      })
      .from(cashMovements)
      .leftJoin(users, eq(cashMovements.userId, users.id))
      .leftJoin(customers, eq(cashMovements.customerId, customers.id))
      .where(eq(cashMovements.clientId, clientId))
      .orderBy(desc(cashMovements.createdAt));
  }

  async getCashMovementsByType(clientId: number, type: string): Promise<CashMovement[]> {
    return await db
      .select({
        id: cashMovements.id,
        clientId: cashMovements.clientId,
        cashRegisterId: cashMovements.cashRegisterId,
        type: cashMovements.type,
        subtype: cashMovements.subtype,
        amount: cashMovements.amount,
        currency: cashMovements.currency,
        exchangeRate: cashMovements.exchangeRate,
        amountUsd: cashMovements.amountUsd,
        description: cashMovements.description,
        referenceId: cashMovements.referenceId,
        referenceType: cashMovements.referenceType,
        customerId: cashMovements.customerId,
        vendorId: cashMovements.vendorId,
        userId: cashMovements.userId,
        notes: cashMovements.notes,
        createdAt: cashMovements.createdAt,
        userName: users.username,
        customerName: customers.name
      })
      .from(cashMovements)
      .leftJoin(users, eq(cashMovements.userId, users.id))
      .leftJoin(customers, eq(cashMovements.customerId, customers.id))
      .where(and(
        eq(cashMovements.clientId, clientId),
        eq(cashMovements.type, type)
      ))
      .orderBy(desc(cashMovements.createdAt));
  }

  async getCashMovementsByDateRange(clientId: number, startDate: Date, endDate: Date): Promise<CashMovement[]> {
    return await db
      .select({
        id: cashMovements.id,
        clientId: cashMovements.clientId,
        cashRegisterId: cashMovements.cashRegisterId,
        type: cashMovements.type,
        subtype: cashMovements.subtype,
        amount: cashMovements.amount,
        currency: cashMovements.currency,
        exchangeRate: cashMovements.exchangeRate,
        amountUsd: cashMovements.amountUsd,
        description: cashMovements.description,
        referenceId: cashMovements.referenceId,
        referenceType: cashMovements.referenceType,
        customerId: cashMovements.customerId,
        vendorId: cashMovements.vendorId,
        userId: cashMovements.userId,
        notes: cashMovements.notes,
        createdAt: cashMovements.createdAt,
        userName: users.username,
        customerName: customers.name
      })
      .from(cashMovements)
      .leftJoin(users, eq(cashMovements.userId, users.id))
      .leftJoin(customers, eq(cashMovements.customerId, customers.id))
      .where(and(
        eq(cashMovements.clientId, clientId),
        gte(cashMovements.createdAt, startDate),
        lt(cashMovements.createdAt, endDate)
      ))
      .orderBy(desc(cashMovements.createdAt));
  }

  async getCashMovementsWithFilters(clientId: number, filters: {
    type?: string;
    dateFrom?: Date;
    dateTo?: Date;
    customer?: string;
    vendor?: string;
    search?: string;
    paymentMethod?: string;
  }): Promise<CashMovement[]> {
    let query = db
      .select({
        id: cashMovements.id,
        clientId: cashMovements.clientId,
        cashRegisterId: cashMovements.cashRegisterId,
        type: cashMovements.type,
        subtype: cashMovements.subtype,
        amount: cashMovements.amount,
        currency: cashMovements.currency,
        exchangeRate: cashMovements.exchangeRate,
        amountUsd: cashMovements.amountUsd,
        description: cashMovements.description,
        referenceId: cashMovements.referenceId,
        referenceType: cashMovements.referenceType,
        customerId: cashMovements.customerId,
        vendorId: cashMovements.vendorId,
        userId: cashMovements.userId,
        notes: cashMovements.notes,
        createdAt: cashMovements.createdAt,
        userName: users.username,
        customerName: customers.name,
        vendorName: vendors.name
      })
      .from(cashMovements)
      .leftJoin(users, eq(cashMovements.userId, users.id))
      .leftJoin(customers, eq(cashMovements.customerId, customers.id))
      .leftJoin(vendors, eq(cashMovements.vendorId, vendors.id));

    const conditions = [eq(cashMovements.clientId, clientId)];

    // Apply filters
    if (filters.type) {
      conditions.push(eq(cashMovements.type, filters.type));
    }

    if (filters.dateFrom && filters.dateTo) {
      conditions.push(gte(cashMovements.createdAt, filters.dateFrom));
      conditions.push(lt(cashMovements.createdAt, filters.dateTo));
    }

    if (filters.customer) {
      conditions.push(ilike(customers.name, `%${filters.customer}%`));
    }

    if (filters.vendor) {
      conditions.push(
        or(
          ilike(vendors.name, `%${filters.vendor}%`),
          ilike(users.username, `%${filters.vendor}%`)
        )
      );
    }

    if (filters.search) {
      conditions.push(
        or(
          ilike(cashMovements.description, `%${filters.search}%`),
          ilike(cashMovements.notes, `%${filters.search}%`),
          ilike(customers.name, `%${filters.search}%`),
          ilike(users.username, `%${filters.search}%`),
          ilike(vendors.name, `%${filters.search}%`)
        )
      );
    }

    if (filters.paymentMethod) {
      conditions.push(eq(cashMovements.subtype, filters.paymentMethod));
    }

    return await query
      .where(and(...conditions))
      .orderBy(desc(cashMovements.createdAt));
  }

  async getAllCashMovementsForExport(clientId: number): Promise<CashMovement[]> {
    return await db
      .select({
        id: cashMovements.id,
        clientId: cashMovements.clientId,
        cashRegisterId: cashMovements.cashRegisterId,
        type: cashMovements.type,
        subtype: cashMovements.subtype,
        amount: cashMovements.amount,
        currency: cashMovements.currency,
        exchangeRate: cashMovements.exchangeRate,
        amountUsd: cashMovements.amountUsd,
        description: cashMovements.description,
        referenceId: cashMovements.referenceId,
        referenceType: cashMovements.referenceType,
        customerId: cashMovements.customerId,
        vendorId: cashMovements.vendorId,
        userId: cashMovements.userId,
        notes: cashMovements.notes,
        createdAt: cashMovements.createdAt,
        userName: users.username,
        customerName: customers.name,
        vendorName: vendors.name
      })
      .from(cashMovements)
      .leftJoin(users, eq(cashMovements.userId, users.id))
      .leftJoin(customers, eq(cashMovements.customerId, customers.id))
      .leftJoin(vendors, eq(cashMovements.vendorId, vendors.id))
      .where(eq(cashMovements.clientId, clientId))
      .orderBy(desc(cashMovements.createdAt));
  }

  // =======================
  // EXPENSES
  // =======================

  async createExpense(expense: InsertExpense): Promise<Expense> {
    const [result] = await db.insert(expenses).values(expense).returning();
    return result;
  }

  async getExpensesByClientId(clientId: number): Promise<Expense[]> {
    return await db
      .select({
        id: expenses.id,
        clientId: expenses.clientId,
        cashRegisterId: expenses.cashRegisterId,
        category: expenses.category,
        description: expenses.description,
        amount: expenses.amount,
        currency: expenses.currency,
        exchangeRate: expenses.exchangeRate,
        amountUsd: expenses.amountUsd,
        paymentMethod: expenses.paymentMethod,
        providerId: expenses.providerId,
        userId: expenses.userId,
        receiptNumber: expenses.receiptNumber,
        notes: expenses.notes,
        expenseDate: expenses.expenseDate,
        createdAt: expenses.createdAt,
        userName: users.username
      })
      .from(expenses)
      .leftJoin(users, eq(expenses.userId, users.id))
      .where(eq(expenses.clientId, clientId))
      .orderBy(desc(expenses.createdAt));
  }

  async getExpensesByCategory(clientId: number, category: string): Promise<Expense[]> {
    return await db
      .select({
        id: expenses.id,
        clientId: expenses.clientId,
        cashRegisterId: expenses.cashRegisterId,
        category: expenses.category,
        description: expenses.description,
        amount: expenses.amount,
        currency: expenses.currency,
        exchangeRate: expenses.exchangeRate,
        amountUsd: expenses.amountUsd,
        paymentMethod: expenses.paymentMethod,
        providerId: expenses.providerId,
        userId: expenses.userId,
        receiptNumber: expenses.receiptNumber,
        notes: expenses.notes,
        expenseDate: expenses.expenseDate,
        createdAt: expenses.createdAt,
        userName: users.username
      })
      .from(expenses)
      .leftJoin(users, eq(expenses.userId, users.id))
      .where(and(
        eq(expenses.clientId, clientId),
        eq(expenses.category, category)
      ))
      .orderBy(desc(expenses.createdAt));
  }

  async getExpensesByDateRange(clientId: number, startDate: Date, endDate: Date): Promise<Expense[]> {
    return await db
      .select({
        id: expenses.id,
        clientId: expenses.clientId,
        cashRegisterId: expenses.cashRegisterId,
        category: expenses.category,
        description: expenses.description,
        amount: expenses.amount,
        currency: expenses.currency,
        exchangeRate: expenses.exchangeRate,
        amountUsd: expenses.amountUsd,
        paymentMethod: expenses.paymentMethod,
        providerId: expenses.providerId,
        userId: expenses.userId,
        receiptNumber: expenses.receiptNumber,
        notes: expenses.notes,
        expenseDate: expenses.expenseDate,
        createdAt: expenses.createdAt,
        userName: users.username
      })
      .from(expenses)
      .leftJoin(users, eq(expenses.userId, users.id))
      .where(and(
        eq(expenses.clientId, clientId),
        gte(expenses.expenseDate, startDate),
        lt(expenses.expenseDate, endDate)
      ))
      .orderBy(desc(expenses.createdAt));
  }

  async updateExpense(id: number, updates: Partial<InsertExpense>): Promise<Expense | undefined> {
    const [result] = await db
      .update(expenses)
      .set(updates)
      .where(eq(expenses.id, id))
      .returning();
    return result;
  }

  async deleteExpense(id: number): Promise<boolean> {
    const result = await db.delete(expenses).where(eq(expenses.id, id));
    return result.rowCount > 0;
  }

  // =======================
  // CUSTOMER DEBTS
  // =======================

  async createCustomerDebt(debt: InsertCustomerDebt): Promise<CustomerDebt> {
    const [result] = await db.insert(customerDebts).values(debt).returning();
    return result;
  }

  async getCustomerDebtsByClientId(clientId: number): Promise<CustomerDebt[]> {
    return await db
      .select({
        id: customerDebts.id,
        clientId: customerDebts.clientId,
        customerId: customerDebts.customerId,
        orderId: customerDebts.orderId,
        debtAmount: customerDebts.debtAmount,
        paidAmount: customerDebts.paidAmount,
        remainingAmount: customerDebts.remainingAmount,
        currency: customerDebts.currency,
        status: customerDebts.status,
        dueDate: customerDebts.dueDate,
        paymentHistory: customerDebts.paymentHistory,
        notes: customerDebts.notes,
        createdAt: customerDebts.createdAt,
        updatedAt: customerDebts.updatedAt,
        customerName: customers.name
      })
      .from(customerDebts)
      .leftJoin(customers, eq(customerDebts.customerId, customers.id))
      .where(eq(customerDebts.clientId, clientId))
      .orderBy(desc(customerDebts.createdAt));
  }

  async getCustomerDebtsByCustomerId(customerId: number): Promise<CustomerDebt[]> {
    return await db
      .select({
        id: customerDebts.id,
        clientId: customerDebts.clientId,
        customerId: customerDebts.customerId,
        orderId: customerDebts.orderId,
        debtAmount: customerDebts.debtAmount,
        paidAmount: customerDebts.paidAmount,
        remainingAmount: customerDebts.remainingAmount,
        currency: customerDebts.currency,
        status: customerDebts.status,
        dueDate: customerDebts.dueDate,
        paymentHistory: customerDebts.paymentHistory,
        notes: customerDebts.notes,
        createdAt: customerDebts.createdAt,
        updatedAt: customerDebts.updatedAt,
        customerName: customers.name
      })
      .from(customerDebts)
      .leftJoin(customers, eq(customerDebts.customerId, customers.id))
      .where(eq(customerDebts.customerId, customerId))
      .orderBy(desc(customerDebts.createdAt));
  }

  async getActiveDebts(clientId: number): Promise<CustomerDebt[]> {
    return await db
      .select({
        id: customerDebts.id,
        clientId: customerDebts.clientId,
        customerId: customerDebts.customerId,
        orderId: customerDebts.orderId,
        debtAmount: customerDebts.debtAmount,
        paidAmount: customerDebts.paidAmount,
        remainingAmount: customerDebts.remainingAmount,
        currency: customerDebts.currency,
        status: customerDebts.status,
        dueDate: customerDebts.dueDate,
        paymentHistory: customerDebts.paymentHistory,
        notes: customerDebts.notes,
        createdAt: customerDebts.createdAt,
        updatedAt: customerDebts.updatedAt,
        customerName: customers.name
      })
      .from(customerDebts)
      .leftJoin(customers, eq(customerDebts.customerId, customers.id))
      .where(and(
        eq(customerDebts.clientId, clientId),
        eq(customerDebts.status, 'vigente')
      ))
      .orderBy(desc(customerDebts.createdAt));
  }

  async getActiveDebtByOrderId(orderId: number): Promise<CustomerDebt | undefined> {
    const [debt] = await db.select()
      .from(customerDebts)
      .where(and(
        eq(customerDebts.orderId, orderId),
        eq(customerDebts.status, 'vigente')
      ))
      .limit(1);
    return debt;
  }

  async updateCustomerDebt(id: number, updates: Partial<InsertCustomerDebt>): Promise<CustomerDebt | undefined> {
    const [result] = await db
      .update(customerDebts)
      .set(updates)
      .where(eq(customerDebts.id, id))
      .returning();
    return result;
  }

  // =======================
  // DEBT PAYMENTS
  // =======================

  async createDebtPayment(payment: InsertDebtPayment): Promise<DebtPayment> {
    const [result] = await db.insert(debtPayments).values(payment).returning();
    return result;
  }

  async getDebtPaymentsByDebtId(debtId: number): Promise<DebtPayment[]> {
    return await db
      .select({
        id: debtPayments.id,
        clientId: debtPayments.clientId,
        debtId: debtPayments.debtId,
        cashRegisterId: debtPayments.cashRegisterId,
        amount: debtPayments.amount,
        currency: debtPayments.currency,
        exchangeRate: debtPayments.exchangeRate,
        amountUsd: debtPayments.amountUsd,
        paymentMethod: debtPayments.paymentMethod,
        userId: debtPayments.userId,
        notes: debtPayments.notes,
        paymentDate: debtPayments.paymentDate,
        createdAt: debtPayments.createdAt,
        userName: users.username
      })
      .from(debtPayments)
      .leftJoin(users, eq(debtPayments.userId, users.id))
      .where(eq(debtPayments.debtId, debtId))
      .orderBy(desc(debtPayments.createdAt));
  }

  async getDebtPaymentsByClientId(clientId: number): Promise<DebtPayment[]> {
    return await db
      .select({
        id: debtPayments.id,
        clientId: debtPayments.clientId,
        debtId: debtPayments.debtId,
        cashRegisterId: debtPayments.cashRegisterId,
        amount: debtPayments.amount,
        currency: debtPayments.currency,
        exchangeRate: debtPayments.exchangeRate,
        amountUsd: debtPayments.amountUsd,
        paymentMethod: debtPayments.paymentMethod,
        userId: debtPayments.userId,
        notes: debtPayments.notes,
        paymentDate: debtPayments.paymentDate,
        createdAt: debtPayments.createdAt,
        userName: users.username
      })
      .from(debtPayments)
      .leftJoin(users, eq(debtPayments.userId, users.id))
      .where(eq(debtPayments.clientId, clientId))
      .orderBy(desc(debtPayments.createdAt));
  }

  // =======================
  // DAILY REPORTS
  // =======================

  async createDailyReport(report: InsertDailyReport): Promise<DailyReport> {
    const [result] = await db.insert(dailyReports).values(report).returning();
    return result;
  }

  async getDailyReportsByClientId(clientId: number): Promise<DailyReport[]> {
    return await db
      .select()
      .from(dailyReports)
      .where(eq(dailyReports.clientId, clientId))
      .orderBy(desc(dailyReports.reportDate));
  }

  async getDailyReportByDate(clientId: number, date: Date): Promise<DailyReport | undefined> {
    const [result] = await db
      .select()
      .from(dailyReports)
      .where(and(
        eq(dailyReports.clientId, clientId),
        eq(dailyReports.reportDate, date)
      ));
    return result;
  }

  async generateAutoDailyReport(clientId: number, date: Date): Promise<DailyReport> {
    // Get all movements for the day
    const startDate = new Date(date);
    startDate.setHours(0, 0, 0, 0);
    const endDate = new Date(date);
    endDate.setHours(23, 59, 59, 999);

    // Calculate totals using raw SQL for better performance
    const totalsQuery = await db.execute(sqlOperator`
      SELECT
        COALESCE(SUM(CASE WHEN cm.type IN ('ingreso', 'venta') THEN CAST(cm.amount_usd AS DECIMAL) ELSE 0 END), 0) as total_income,
        COALESCE(SUM(CASE WHEN e.id IS NOT NULL THEN CAST(e.amount_usd AS DECIMAL) ELSE 0 END), 0) as total_expenses,
        COALESCE(SUM(CASE WHEN dp.id IS NOT NULL THEN CAST(dp.amount_usd AS DECIMAL) ELSE 0 END), 0) as total_debt_payments,
        COUNT(cm.id) as total_movements
      FROM cash_movements cm
      LEFT JOIN expenses e ON e.client_id = ${clientId} AND e.expense_date >= ${startDate} AND e.expense_date < ${endDate}
      LEFT JOIN debt_payments dp ON dp.client_id = ${clientId} AND dp.payment_date >= ${startDate} AND dp.payment_date < ${endDate}
      WHERE cm.client_id = ${clientId}
        AND cm.created_at >= ${startDate}
        AND cm.created_at < ${endDate}
    `);

    const totals = totalsQuery.rows[0] as any;
    const totalIncome = parseFloat(totals.total_income || "0");
    const totalExpenses = parseFloat(totals.total_expenses || "0");
    const totalDebtPayments = parseFloat(totals.total_debt_payments || "0");
    const netProfit = totalIncome - totalExpenses;

    const reportData = {
      summary: {
        totalIncome,
        totalExpenses,
        totalDebtPayments,
        netProfit,
        movementsCount: parseInt(totals.total_movements || "0")
      },
      timestamp: new Date().toISOString()
    };

    const report = await this.createDailyReport({
      clientId,
      reportDate: date,
      openingBalance: "1000.00", // Should get from previous day's closing
      totalIncome: totalIncome.toFixed(2),
      totalExpenses: totalExpenses.toFixed(2),
      totalDebts: "0.00", // Calculate active debts
      totalDebtPayments: totalDebtPayments.toFixed(2),
      netProfit: netProfit.toFixed(2),
      vendorCommissions: "0.00", // Calculate vendor commissions
      exchangeRateUsed: "1200.00", // Get current rate
      closingBalance: (1000 + netProfit).toFixed(2),
      totalMovements: parseInt(totals.total_movements || "0"),
      reportData: JSON.stringify(reportData),
      isAutoGenerated: true
    });

    return report;
  }

  // =======================
  // REAL-TIME STATE
  // =======================

  async getRealTimeCashState(clientId: number): Promise<any> {
    try {
      // Validate client ID
      if (!clientId || clientId <= 0) {
        throw new Error('Invalid client ID');
      }

      console.log('🔄 getRealTimeCashState: Starting optimized calculation for clientId:', clientId);

      // Use aggregated queries for better performance - FIXED ITERATION ERROR
      const incomeQuery = await db.execute(sqlOperator`
        SELECT
          COALESCE(SUM(CASE WHEN type IN ('venta', 'ingreso', 'pago_deuda') THEN CAST(amount_usd AS DECIMAL) ELSE 0 END), 0) as total_income,
          COALESCE(SUM(CASE WHEN type IN ('gasto', 'egreso') THEN CAST(amount_usd AS DECIMAL) ELSE 0 END), 0) as total_expenses,
          COALESCE(SUM(CASE WHEN subtype = 'efectivo_ars' AND type IN ('venta', 'ingreso', 'pago_deuda') THEN CAST(amount AS DECIMAL) ELSE 0 END), 0) as efectivo_ars,
          COALESCE(SUM(CASE WHEN subtype = 'efectivo_usd' AND type IN ('venta', 'ingreso', 'pago_deuda') THEN CAST(amount_usd AS DECIMAL) ELSE 0 END), 0) as efectivo_usd,
          COALESCE(SUM(CASE WHEN subtype = 'transferencia_ars' AND type IN ('venta', 'ingreso', 'pago_deuda') THEN CAST(amount AS DECIMAL) ELSE 0 END), 0) as transferencia_ars,
          COALESCE(SUM(CASE WHEN subtype = 'transferencia_usd' AND type IN ('venta', 'ingreso', 'pago_deuda') THEN CAST(amount_usd AS DECIMAL) ELSE 0 END), 0) as transferencia_usd,
          COALESCE(SUM(CASE WHEN subtype = 'transferencia_usdt' AND type IN ('venta', 'ingreso', 'pago_deuda') THEN CAST(amount_usd AS DECIMAL) ELSE 0 END), 0) as transferencia_usdt,
          COALESCE(SUM(CASE WHEN subtype = 'financiera_ars' AND type IN ('venta', 'ingreso', 'pago_deuda') THEN CAST(amount AS DECIMAL) ELSE 0 END), 0) as financiera_ars,
          COALESCE(SUM(CASE WHEN subtype = 'financiera_usd' AND type IN ('venta', 'ingreso', 'pago_deuda') THEN CAST(amount_usd AS DECIMAL) ELSE 0 END), 0) as financiera_usd,
          COUNT(*) as total_movements
        FROM cash_movements
        WHERE client_id = ${clientId}
      `);

      // CORRECCIÓN CRÍTICA: Acceder correctamente al primer resultado
      const aggregatedData = incomeQuery.rows[0] as any;
      const totalIncome = parseFloat(aggregatedData.total_income || "0");
      const totalExpenses = parseFloat(aggregatedData.total_expenses || "0");

      let salesByPaymentMethod = {
        total_ventas: totalIncome,
        total_gastos: totalExpenses,
        efectivo_ars: parseFloat(aggregatedData.efectivo_ars || "0"),
        efectivo_usd: parseFloat(aggregatedData.efectivo_usd || "0"),
        transferencia_ars: parseFloat(aggregatedData.transferencia_ars || "0"),
        transferencia_usd: parseFloat(aggregatedData.transferencia_usd || "0"),
        transferencia_usdt: parseFloat(aggregatedData.transferencia_usdt || "0"),
        financiera_ars: parseFloat(aggregatedData.financiera_ars || "0"),
        financiera_usd: parseFloat(aggregatedData.financiera_usd || "0"),
        balance_final: 0,
        deudas_pendientes: 0
      };

      // Get opening balance from cash register
      const currentCashRegister = await this.getCurrentCashRegister(clientId);
      const openingBalance = parseFloat(currentCashRegister?.initialUsd || "1000");

      // Calculate active debts
      let totalActiveDebts = 0;
      try {
        totalActiveDebts = await this.getTotalDebtsAmount(clientId);
        console.log('✅ Active debts calculated:', totalActiveDebts);
      } catch (error) {
        console.error('❌ Error calculating debts:', error);
        totalActiveDebts = 0;
      }

      salesByPaymentMethod.deudas_pendientes = totalActiveDebts;
      salesByPaymentMethod.balance_final = openingBalance + salesByPaymentMethod.total_ventas - salesByPaymentMethod.total_gastos;

      console.log('🎯 Final Results:', salesByPaymentMethod);

      return {
        totalBalanceUsd: salesByPaymentMethod.balance_final.toFixed(2),
        dailySalesUsd: salesByPaymentMethod.total_ventas.toFixed(2),
        dailyExpensesUsd: salesByPaymentMethod.total_gastos.toFixed(2),
        totalActiveDebtsUsd: totalActiveDebts.toFixed(2),
        openingBalance: openingBalance.toFixed(2),
        salesByPaymentMethod,
        lastUpdated: new Date().toISOString()
      };
    } catch (error) {
      console.error('❌ Error in getRealTimeCashState:', error);
      // Return safe defaults
      return {
        totalBalanceUsd: "1000.00",
        dailySalesUsd: "0.00",
        dailyExpensesUsd: "0.00",
        totalActiveDebtsUsd: "0.00",
        openingBalance: "1000.00",
        salesByPaymentMethod: {
          total_ventas: 0,
          total_gastos: 0,
          efectivo_ars: 0,
          efectivo_usd: 0,
          transferencia_ars: 0,
          transferencia_usd: 0,
          transferencia_usdt: 0,
          financiera_ars: 0,
          financiera_usd: 0,
          balance_final: 1000,
          deudas_pendientes: 0
        },
        lastUpdated: new Date().toISOString()
      };
    }
  }

  async getTotalDebtsAmount(clientId: number): Promise<number> {
    console.log('🔍 Calculating total debts for clientId:', clientId);

    // Get real debts from the database
    const activeDebts = await this.getActiveDebts(clientId);
    const totalDebts = activeDebts.reduce((sum, debt) => {
      return sum + parseFloat(debt.remainingAmount || '0');
    }, 0);

    console.log('📊 Total active debts calculated:', totalDebts);
    return totalDebts;
  }

  async getTotalPendingVendorPayments(clientId: number): Promise<number> {
    // Implementation for vendor payments if needed
    return 0;
  }

  async getStockValue(clientId: number): Promise<{usd: number, ars: number}> {
    const result = await db.execute(sqlOperator`
      SELECT COALESCE(SUM(CAST(cost_price AS DECIMAL)), 0) as total
      FROM products
      WHERE client_id = ${clientId} AND status IN ('disponible', 'reservado')
    `);

    const totalUsd = parseFloat(result.rows[0]?.total || "0");
    const exchangeRate = 1200; // Should get from current rate

    return {
      usd: totalUsd,
      ars: totalUsd * exchangeRate
    };
  }

  async getOrCreateTodayCashRegister(clientId: number): Promise<CashRegister> {
    try {
      const today = new Date();
      today.setHours(0, 0, 0, 0);

      // Try to get current cash register
      let cashRegister = await this.getCurrentCashRegister(clientId);

      // If there's an open cash register from a previous day, close it first
      if (cashRegister && !this.isSameDay(new Date(cashRegister.date), today)) {
        console.log('🔄 Auto-closing previous day cash register before opening new one');
        await this.autoCloseCashRegister(clientId);
        cashRegister = null; // Force creation of new register
      }

      // If no cash register for today, create one automatically
      if (!cashRegister) {
        // Get previous day's closing balance to use as opening balance
        const previousClosingBalance = await this.getPreviousDayClosingBalance(clientId);
        const initialUsd = previousClosingBalance.toString();
        const initialArs = "0.00";
        const initialUsdt = "0.00";

        console.log(`💰 Creating new cash register with opening balance: $${initialUsd}`);

        cashRegister = await this.createCashRegister({
          clientId,
          date: today,
          initialUsd,
          initialArs,
          initialUsdt,
          currentUsd: initialUsd,
          currentArs: initialArs,
          currentUsdt: initialUsdt,
          dailySales: "0.00",
          totalDebts: "0.00",
          totalExpenses: "0.00",
          dailyGlobalExchangeRate: "1200.00",
          isOpen: true,
          isActive: true
        });
      }

      return cashRegister;
    } catch (error) {
      console.error('Error in getOrCreateTodayCashRegister:', error);
      throw new Error('No se pudo obtener o crear la caja del día');
    }
  }

  async autoCloseCashRegister(clientId: number): Promise<CashRegister | undefined> {
    const currentRegister = await this.getCurrentCashRegister(clientId);
    if (!currentRegister) return undefined;

    // Calculate closing values
    const realTimeState = await this.getRealTimeCashState(clientId);

    // Create daily report before closing
    await this.createAutoDailyReport(clientId, currentRegister, realTimeState);

    const updatedRegister = await this.updateCashRegister(currentRegister.id, {
      isOpen: false,
      closedAt: new Date(),
      autoClosedAt: new Date(),
      currentUsd: realTimeState.totalBalanceUsd,
      dailySales: realTimeState.dailySalesUsd,
      totalExpenses: realTimeState.dailyExpensesUsd,
      totalDebts: realTimeState.totalActiveDebtsUsd
    });

    return updatedRegister;
  }

  async createAutoDailyReport(clientId: number, cashRegister: any, realTimeState: any): Promise<void> {
    try {
      // Get current time in Argentina timezone (UTC-3)
      const now = new Date();
      const argentinaOffset = -3 * 60; // UTC-3 in minutes
      const argentinaTime = new Date(now.getTime() + (argentinaOffset - now.getTimezoneOffset()) * 60 * 1000);

      // Get all movements from today
      const todayStart = new Date(argentinaTime.getFullYear(), argentinaTime.getMonth(), argentinaTime.getDate());
      const todayEnd = new Date(todayStart.getTime() + 24 * 60 * 60 * 1000 - 1);

      const movements = await db.select()
        .from(cashMovements)
        .where(and(
          eq(cashMovements.clientId, clientId),
          gte(cashMovements.createdAt, todayStart),
          lt(cashMovements.createdAt, todayEnd)
        ));

      // Calculate totals
      let totalIncome = 0;
      let totalExpenses = 0;
      let totalDebtPayments = 0;

      movements.forEach(movement => {
        const amountUsd = parseFloat(movement.amountUsd);
        if (movement.type === 'venta' || movement.type === 'ingreso') {
          totalIncome += amountUsd;
        } else if (movement.type === 'gasto') {
          totalExpenses += Math.abs(amountUsd);
        } else if (movement.type === 'pago_deuda') {
          totalDebtPayments += amountUsd;
        }
      });

      const netProfit = totalIncome - totalExpenses;
      const openingBalance = parseFloat(cashRegister.initialUsd || "0");
      const closingBalance = parseFloat(realTimeState.totalBalanceUsd);

      // Create daily report
      const [dailyReport] = await db.insert(dailyReports).values({
        clientId,
        reportDate: argentinaTime,
        openingBalance: openingBalance.toString(),
        totalIncome: totalIncome.toString(),
        totalExpenses: totalExpenses.toString(),
        totalDebts: realTimeState.totalActiveDebtsUsd,
        totalDebtPayments: totalDebtPayments.toString(),
        netProfit: netProfit.toString(),
        vendorCommissions: "0", // TODO: Calculate vendor commissions
        exchangeRateUsed: "1200", // TODO: Get from configuration
        closingBalance: closingBalance.toString(),
        totalMovements: movements.length,
        reportData: JSON.stringify({
          movements: movements.slice(0, 100), // Store first 100 movements
          summary: {
            totalIncome,
            totalExpenses,
            totalDebtPayments,
            netProfit,
            openingBalance,
            closingBalance
          }
        }),
        isAutoGenerated: true
      }).returning();

      // Generate automatic Excel report
      if (dailyReport) {
        await this.generateAutoExcelReport(clientId, dailyReport.id, argentinaTime);
      }

      console.log(`📊 Daily report created for client ${clientId} - ${argentinaTime.toISOString().split('T')[0]}`);
      console.log(`📊 Excel report generated automatically for ${argentinaTime.toISOString().split('T')[0]}`);
    } catch (error) {
      console.error('❌ Error creating daily report:', error);
      throw error;
    }
  }

  // Helper method to check if two dates are the same day
  private isSameDay(date1: Date, date2: Date): boolean {
    return date1.getFullYear() === date2.getFullYear() &&
           date1.getMonth() === date2.getMonth() &&
           date1.getDate() === date2.getDate();
  }

  // Get previous day's closing balance to use as opening balance for new day
  async getPreviousDayClosingBalance(clientId: number): Promise<number> {
    try {
      // Try to get the most recent daily report
      const [lastReport] = await db.select()
        .from(dailyReports)
        .where(eq(dailyReports.clientId, clientId))
        .orderBy(desc(dailyReports.reportDate))
        .limit(1);

      if (lastReport && lastReport.closingBalance) {
        const balance = parseFloat(lastReport.closingBalance);
        console.log(`📊 Using previous day closing balance: $${balance}`);
        return balance;
      }

      // If no previous report, try to get the last closed cash register
      const [lastClosedRegister] = await db.select()
        .from(cashRegister)
        .where(and(
          eq(cashRegister.clientId, clientId),
          eq(cashRegister.isOpen, false)
        ))
        .orderBy(desc(cashRegister.closedAt))
        .limit(1);

      if (lastClosedRegister && lastClosedRegister.currentUsd) {
        const balance = parseFloat(lastClosedRegister.currentUsd);
        console.log(`📊 Using last closed register balance: $${balance}`);
        return balance;
      }

      // Default opening balance
      console.log(`📊 Using default opening balance: $1000.00`);
      return 1000.00;
    } catch (error) {
      console.error('❌ Error getting previous day closing balance:', error);
      return 1000.00; // Safe default
    }
  }

  async getDailyReports(clientId: number, limit: number = 30): Promise<any[]> {
    return await db.select()
      .from(dailyReports)
      .where(eq(dailyReports.clientId, clientId))
      .orderBy(desc(dailyReports.reportDate))
      .limit(limit);
  }

  // =======================
  // GENERATED REPORTS
  // =======================

  async createGeneratedReport(report: InsertGeneratedReport): Promise<GeneratedReport> {
    const [result] = await db.insert(generatedReports).values(report).returning();
    return result;
  }

  async getGeneratedReportsByClientId(clientId: number): Promise<GeneratedReport[]> {
    return await db
      .select()
      .from(generatedReports)
      .leftJoin(dailyReports, eq(generatedReports.dailyReportId, dailyReports.id))
      .where(eq(generatedReports.clientId, clientId))
      .orderBy(desc(generatedReports.generatedAt));
  }

  async getGeneratedReportById(id: number): Promise<GeneratedReport | undefined> {
    const [result] = await db
      .select()
      .from(generatedReports)
      .where(eq(generatedReports.id, id));
    return result;
  }

  async generateAutoExcelReport(clientId: number, dailyReportId: number, reportDate: Date): Promise<void> {
    try {
      // Get comprehensive data for the report
      const [
        cashMovements,
        expenses,
        debtPayments,
        vendorPerformance,
        paymentMethods // This seems to be missing in the query, assuming it's for payment method counts
      ] = await Promise.all([
        this.getAllCashMovementsForExport(clientId),
        this.getExpensesByClientId(clientId),
        this.getDebtPaymentsByClientId(clientId),
        this.getVendorPerformanceRanking(clientId),
        this.getPaymentMethodsSummary(clientId) // Assuming this method exists or needs to be created
      ]);

      // Filter data for the specific date
      const dateStr = reportDate.toISOString().split('T')[0];
      const filteredMovements = cashMovements.filter((mov: any) =>
        mov.createdAt.toISOString().split('T')[0] === dateStr
      );
      const filteredExpenses = expenses.filter((exp: any) =>
        exp.expenseDate.toISOString().split('T')[0] === dateStr
      );
      const filteredDebtPayments = debtPayments.filter((debt: any) =>
        debt.paymentDate.toISOString().split('T')[0] === dateStr
      );

      // Calculate payment method breakdown
      const paymentMethodBreakdown: any = {};
      filteredMovements.forEach((movement: any) => {
        const method = movement.subtype || 'No especificado';
        if (!paymentMethodBreakdown[method]) {
          paymentMethodBreakdown[method] = { count: 0, totalUsd: 0 };
        }
        paymentMethodBreakdown[method].count++;
        paymentMethodBreakdown[method].totalUsd += parseFloat(movement.amountUsd || 0);
      });

      // Prepare comprehensive report content
      const reportContent = {
        date: dateStr,
        paymentMethodBreakdown,
        detailedPayments: filteredMovements,
        detailedExpenses: filteredExpenses,
        cashMovements: filteredMovements,
        vendorPerformance: vendorPerformance,
        summary: {
          totalIncome: filteredMovements
            .filter((m: any) => m.type === 'venta' || m.type === 'ingreso')
            .reduce((sum: number, m: any) => sum + parseFloat(m.amountUsd || 0), 0),
          totalExpenses: filteredExpenses
            .reduce((sum: number, e: any) => sum + parseFloat(e.amountUsd || 0), 0),
          totalDebtPayments: filteredDebtPayments
            .reduce((sum: number, d: any) => sum + parseFloat(d.amountUsd || 0), 0),
          totalMovements: filteredMovements.length
        }
      };

      // Generate Excel content as CSV format
      const excelContent = this.generateExcelCsvContent(reportContent);
      const fileName = `StockCel_Reporte_Diario_${dateStr.replace(/-/g, '_')}.csv`;

      // Store the generated report
      await this.createGeneratedReport({
        clientId,
        dailyReportId,
        reportDate,
        reportType: 'excel',
        fileName,
        fileData: Buffer.from(excelContent).toString('base64'),
        fileSize: Buffer.from(excelContent).length,
        reportContent: JSON.stringify(reportContent),
        isAutoGenerated: true
      });

      console.log(`📊 Excel report generated automatically: ${fileName}`);
    } catch (error) {
      console.error('Error generating auto Excel report:', error);
    }
  }

  // Placeholder for getPaymentMethodsSummary, replace with actual implementation if needed
  private async getPaymentMethodsSummary(clientId: number): Promise<any> {
    console.warn("getPaymentMethodsSummary is not implemented, returning empty object.");
    return {};
  }

  private generateExcelCsvContent(reportContent: any): string {
    const lines = [];

    // Header
    lines.push('STOCKCEL - REPORTE DIARIO AUTOMATICO');
    lines.push(`Fecha: ${reportContent.date}`);
    lines.push('');

    // Summary section
    lines.push('RESUMEN EJECUTIVO');
    lines.push('Concepto,Valor USD');
    lines.push(`Ingresos Totales,${reportContent.summary.totalIncome.toFixed(2)}`);
    lines.push(`Gastos Totales,${reportContent.summary.totalExpenses.toFixed(2)}`);
    lines.push(`Pagos de Deudas,${reportContent.summary.totalDebtPayments.toFixed(2)}`);
    lines.push(`Ganancia Neta,${(reportContent.summary.totalIncome - reportContent.summary.totalExpenses).toFixed(2)}`);
    lines.push(`Total Movimientos,${reportContent.summary.totalMovements}`);
    lines.push('');

    // Payment methods breakdown
    lines.push('DESGLOSE POR METODOS DE PAGO');
    lines.push('Metodo,Cantidad,Total USD');
    Object.entries(reportContent.paymentMethodBreakdown).forEach(([method, data]: any) => {
      lines.push(`${method},${data.count},${data.totalUsd.toFixed(2)}`);
    });
    lines.push('');

    // Detailed movements
    lines.push('MOVIMIENTOS DE CAJA DETALLADOS');
    lines.push('Fecha,Tipo,Descripcion,Metodo,Moneda,Monto Original,Monto USD,Cliente,Vendedor');
    reportContent.detailedPayments.forEach((movement: any) => {
      lines.push([
        new Date(movement.createdAt).toLocaleDateString('es-ES'),
        movement.type,
        movement.description || '-',
        movement.subtype || '-',
        movement.currency,
        movement.amount,
        movement.amountUsd,
        movement.customerName || '-',
        movement.vendorName || '-'
      ].join(','));
    });
    lines.push('');

    // Vendor performance
    lines.push('RENDIMIENTO DE VENDEDORES');
    lines.push('Vendedor,Ventas,Ingresos USD,Ganancia,Comision');
    reportContent.vendorPerformance.forEach((vendor: any) => {
      lines.push([
        vendor.vendorName,
        vendor.totalSales || 0,
        vendor.totalRevenue || 0,
        vendor.totalProfit || 0,
        vendor.commission || 0
      ].join(','));
    });

    return lines.join('\n');
  }

  async getAllCashMovementsForCompleteExport(clientId: number): Promise<any[]> {
    // Get all movements with related data for comprehensive export
    const movements = await db.select({
      id: cashMovements.id,
      type: cashMovements.type,
      subtype: cashMovements.subtype,
      amount: cashMovements.amount,
      currency: cashMovements.currency,
      exchangeRate: cashMovements.exchangeRate,
      amountUsd: cashMovements.amountUsd,
      description: cashMovements.description,
      customerName: customers.name,
      vendorName: vendors.name,
      userId: cashMovements.userId,
      referenceId: cashMovements.referenceId,
      referenceType: cashMovements.referenceType,
      createdAt: cashMovements.createdAt,
      userName: users.username
    })
    .from(cashMovements)
    .leftJoin(users, eq(cashMovements.userId, users.id))
    .leftJoin(customers, eq(cashMovements.customerId, customers.id))
    .leftJoin(vendors, eq(cashMovements.vendorId, vendors.id))
    .where(eq(cashMovements.clientId, clientId))
    .orderBy(desc(cashMovements.createdAt));

    return movements;
  }

  // =======================
  // AUTO-SYNC MONITOR METHODS
  // =======================

  async getOrdersByDate(clientId: number, date: string): Promise<Order[]> {
    const startDate = new Date(date + 'T00:00:00.000Z');
    const endDate = new Date(date + 'T23:59:59.999Z');

    return await db.select()
      .from(orders)
      .where(and(
        eq(orders.clientId, clientId),
        gte(orders.createdAt, startDate),
        lte(orders.createdAt, endDate)
      ))
      .orderBy(desc(orders.createdAt));
  }

  async getCashMovementsByOrderId(orderId: number): Promise<CashMovement[]> {
    return await db.select()
      .from(cashMovements)
      .where(eq(cashMovements.referenceId, orderId));
  }

  // =======================
  // AUTOMATIC CASH SCHEDULING
  // =======================

  async scheduleCashOperations(clientId: number): Promise<{
    status: 'open' | 'closed' | 'no_config' | 'error';
    message?: string;
    nextOpen: Date | null;
    nextClose: Date | null;
    currentTime: string;
    actualCashRegister?: any;
    config?: any;
  }> {
    try {
      console.log(`🔍 [DEBUG] scheduleCashOperations called for clientId: ${clientId}`);

      const config = await cashScheduleStorage.getScheduleConfig(clientId);
      if (!config) {
        return {
          status: 'no_config',
          message: 'No hay configuración de horarios automáticos',
          nextOpen: null,
          nextClose: null,
          currentTime: new Date().toISOString()
        };
      }

      // CORRECCIÓN CRÍTICA: Verificar estado REAL de la caja en la base de datos
      const currentCashRegister = await this.getCurrentCashRegister(clientId);
      const today = new Date().toISOString().split('T')[0];

      // Get current Argentina time
      const now = new Date();
      const argentinaTime = new Date(now.toLocaleString("en-US", { timeZone: "America/Argentina/Buenos_Aires" }));

      console.log(`🕐 Current Argentina time: ${argentinaTime.toLocaleString('es-AR')}`);

      // Calculate next operations
      const currentHour = argentinaTime.getHours();
      const currentMinute = argentinaTime.getMinutes();
      const currentTimeInMinutes = currentHour * 60 + currentMinute;

      const openTimeInMinutes = (config.openHour || 0) * 60 + (config.openMinute || 0);
      const closeTimeInMinutes = (config.closeHour || 23) * 60 + (config.closeMinute || 59);

      // CORRECCIÓN: Determinar estado basado en CAJA REAL + HORARIOS
      let status = 'closed';

      if (currentCashRegister && currentCashRegister.isOpen) {
        // Si hay una caja abierta, verificar que sea de hoy
        const registerDate = new Date(currentCashRegister.date).toISOString().split('T')[0];
        if (registerDate === today) {
          status = 'open';
          console.log(`✅ [SCHEDULE] Cash register is OPEN for today ${today} - ID: ${currentCashRegister.id}`);
        } else {
          status = 'closed';
          console.log(`⚠️ [SCHEDULE] Cash register is from different date ${registerDate}, today is ${today}`);
        }
      } else {
        status = 'closed';
        console.log(`❌ [SCHEDULE] No active cash register found for client ${clientId}`);
      }

      // Calculate next open/close times
      const todayDate = new Date(argentinaTime);
      const tomorrow = new Date(argentinaTime);
      tomorrow.setDate(tomorrow.getDate() + 1);

      let nextOpen = null;
      let nextClose = null;

      if (config.autoOpenEnabled) {
        if (status === 'closed' && currentTimeInMinutes < openTimeInMinutes) {
          // Next open is today
          nextOpen = new Date(todayDate);
          nextOpen.setHours(config.openHour || 0, config.openMinute || 0, 0, 0);
        } else {
          // Next open is tomorrow
          nextOpen = new Date(tomorrow);
          nextOpen.setHours(config.openHour || 0, config.openMinute || 0, 0, 0);
        }
      }

      if (config.autoCloseEnabled) {
        if (status === 'open' && currentTimeInMinutes < closeTimeInMinutes) {
          // Next close is today
          nextClose = new Date(todayDate);
          nextClose.setHours(config.closeHour || 23, config.closeMinute || 59, 0, 0);
        } else {
          // Next close is tomorrow
          nextClose = new Date(tomorrow);
          nextClose.setHours(config.closeHour || 23, config.closeMinute || 59, 0, 0);
        }
      }

      const result = {
        status,
        nextOpen: nextOpen?.toISOString() ?? null,
        nextClose: nextClose?.toISOString() ?? null,
        currentTime: argentinaTime.toISOString(),
        actualCashRegister: currentCashRegister ? {
          id: currentCashRegister.id,
          isOpen: currentCashRegister.isOpen,
          date: currentCashRegister.date
        } : null,
        config: {
          openHour: config.openHour,
          openMinute: config.openMinute,
          closeHour: config.closeHour,
          closeMinute: config.closeMinute,
          autoOpenEnabled: config.autoOpenEnabled,
          autoCloseEnabled: config.autoCloseEnabled
        }
      };

      console.log(`📅 Schedule for client ${clientId}: Status=${status}, Cash Register ID=${currentCashRegister?.id}, isOpen=${currentCashRegister?.isOpen}, Current ${argentinaTime.toLocaleDateString()}, ${argentinaTime.toLocaleTimeString()}, Next Open: ${nextOpen?.toLocaleDateString()}, ${nextOpen?.toLocaleTimeString()}, Next Close: ${nextClose?.toLocaleDateString()}, ${nextClose?.toLocaleTimeString()}`);

      return result;
    } catch (error) {
      console.error('Error getting cash schedule:', error);
      return {
        status: 'error',
        message: 'Error al obtener programación de caja',
        nextOpen: null,
        nextClose: null,
        currentTime: new Date().toISOString()
      };
    }
  }

  async checkAndProcessAutomaticOperations(clientId: number): Promise<{
    closed?: CashRegister;
    opened?: CashRegister;
    notification?: string;
  }> {
    const now = new Date();
    const currentRegister = await this.getCurrentCashRegister(clientId);
    const schedule = await this.scheduleCashOperations(clientId);

    let result: any = {};

    // Check if it's time to close (23:59:00)
    const closeTime = new Date();
    closeTime.setHours(23, 59, 0, 0);

    if (now >= closeTime && currentRegister && currentRegister.isOpen) {
      result.closed = await this.autoCloseCashRegister(clientId);
      result.notification = `🕐 Caja cerrada automáticamente a las ${closeTime.toLocaleTimeString()}. Reabrirá mañana a las 00:00:00`;
    }

    // Check if it's time to open (00:00:00)
    const openTime = new Date();
    openTime.setHours(0, 0, 0, 0);

    if (now >= openTime && (!currentRegister || !currentRegister.isOpen)) {
      // Get previous day's closing balance for opening balance
      const previousBalance = result.closed?.currentUsd || "0.00";

      result.opened = await this.createCashRegister({
        clientId,
        date: new Date(),
        initialUsd: previousBalance,
        initialArs: "0.00",
        initialUsdt: "0.00",
        currentUsd: previousBalance,
        currentArs: "0.00",
        currentUsdt: "0.00",
        dailySales: "0.00",
        totalDebts: "0.00",
        totalExpenses: "0.00",
        dailyGlobalExchangeRate: "1200.00",
        isOpen: true,
        isActive: true
      });

      result.notification = `🌅 Caja abierta automáticamente a las ${openTime.toLocaleTimeString()}. Cerrará hoy a las 23:59:00`;
    }

    return result;
  }

  // Resellers
  async createReseller(reseller: InsertReseller): Promise<Reseller> {
    console.log('🔐 Creando revendedor - Email:', reseller.email);
    console.log('🔐 Password original length:', reseller.password.length);
    console.log('🔐 Password original (primeros 10):', reseller.password.substring(0, 10));

    const hashedPassword = bcrypt.hashSync(reseller.password, 10);
    console.log('🔐 Password hasheado length:', hashedPassword.length);
    console.log('🔐 Password hasheado (primeros 20):', hashedPassword.substring(0, 20));

    const [result] = await db.insert(resellers).values({
      ...reseller,
      password: hashedPassword,
    }).returning();

    console.log('✅ Revendedor creado exitosamente - ID:', result.id);
    return result;
  }

  async getResellers(): Promise<Reseller[]> {
    return await db.select().from(resellers).orderBy(desc(resellers.createdAt));
  }

  async getResellerById(id: number): Promise<Reseller | undefined> {
    const [result] = await db.select().from(resellers).where(eq(resellers.id, id));
    return result;
  }

  async getResellerByEmail(email: string): Promise<Reseller | undefined> {
    const [result] = await db.select().from(resellers).where(eq(resellers.email, email));
    return result;
  }

  async updateReseller(id: number, reseller: Partial<InsertReseller>): Promise<Reseller | undefined> {
    const updateData: any = { ...reseller, updatedAt: new Date() };

    // Hash password if provided
    if (reseller.password) {
      updateData.password = bcrypt.hashSync(reseller.password, 10);
      console.log('🔐 Actualizando revendedor - Password hasheado correctamente');
    }

    const [result] = await db.update(resellers)
      .set(updateData)
      .where(eq(resellers.id, id))
      .returning();
    return result;
  }

  async deleteReseller(id: number): Promise<boolean> {
    // Delete related data first
    await db.delete(resellerSales).where(eq(resellerSales.resellerId, id));
    await db.delete(resellerConfiguration).where(eq(resellerConfiguration.resellerId, id));

    // Delete reseller
    const result = await db.delete(resellers).where(eq(resellers.id, id));
    return result.rowCount > 0;
  }

  // Reseller Sales
  async createResellerSale(resellerId: number, saleData: any): Promise<ResellerSale> {
    // Create client first
    const newClient = await this.createClient({
      name: saleData.clientName,
      email: saleData.clientEmail,
      subscriptionType: saleData.subscriptionType,
      trialStartDate: new Date(),
      trialEndDate: new Date(Date.now() + (saleData.trialDays * 24 * 60 * 60 * 1000)),
    });

    // Calculate profit (salePrice - costPerAccount)
    const profit = saleData.salePrice - saleData.costPerAccount;

    // Create sale record
    const [result] = await db.insert(resellerSales).values({
      resellerId,
      clientId: newClient.id,
      costPerAccount: saleData.costPerAccount.toString(),
      salePrice: saleData.salePrice.toString(),
      profit: profit.toString(),
      subscriptionType: saleData.subscriptionType,
      trialDays: saleData.trialDays,
      notes: saleData.notes,
    }).returning();

    // Update reseller stats - get current values first
    const currentReseller = await db.select().from(resellers).where(eq(resellers.id, resellerId)).limit(1);
    if (currentReseller.length > 0) {
      await db.update(resellers)
        .set({
          accountsSold: currentReseller[0].accountsSold + 1,
          totalEarnings: currentReseller[0].totalEarnings + profit,
          totalPaid: currentReseller[0].totalPaid + saleData.costPerAccount,
          updatedAt: new Date(),
        })
        .where(eq(resellers.id, resellerId));
    }

    return result;
  }

  async getResellerSales(): Promise<ResellerSale[]> {
    const salesWithClients = await db.select({
      id: resellerSales.id,
      resellerId: resellerSales.resellerId,
      clientId: resellerSales.clientId,
      costPerAccount: resellerSales.costPerAccount,
      salePrice: resellerSales.salePrice,
      profit: resellerSales.profit,
      subscriptionType: resellerSales.subscriptionType,
      trialDays: resellerSales.trialDays,
      saleDate: resellerSales.saleDate,
      notes: resellerSales.notes,
      createdAt: resellerSales.createdAt,
      clientName: clients.name,
      clientEmail: clients.email,
    })
    .from(resellerSales)
    .leftJoin(clients, eq(resellerSales.clientId, clients.id))
    .orderBy(desc(resellerSales.saleDate));

    return salesWithClients as any[];
  }

  async getResellerSalesByReseller(resellerId: number): Promise<ResellerSale[]> {
    console.log(`🔍 getResellerSalesByReseller called for resellerId: ${resellerId}`);

    const salesWithClients = await db.select({
      id: resellerSales.id,
      resellerId: resellerSales.resellerId,
      clientId: resellerSales.clientId,
      costPerAccount: resellerSales.costPerAccount,
      salePrice: resellerSales.salePrice,
      profit: resellerSales.profit,
      subscriptionType: resellerSales.subscriptionType,
      trialDays: resellerSales.trialDays,
      saleDate: resellerSales.saleDate,
      notes: resellerSales.notes,
      createdAt: resellerSales.createdAt,
      clientName: clients.name,
      clientEmail: clients.email,
    })
    .from(resellerSales)
    .leftJoin(clients, eq(resellerSales.clientId, clients.id))
    .where(eq(resellerSales.resellerId, resellerId))
    .orderBy(desc(resellerSales.saleDate));

    console.log(`📊 getResellerSalesByReseller result: Found ${salesWithClients.length} sales`);
    console.log(`📊 Sales IDs: ${salesWithClients.map(s => s.id).join(', ')}`);

    return salesWithClients as any[];
  }

  async getClientsByResellerId(resellerId: number): Promise<any[]> {
    // Obtener clientes creados por este revendedor a través de las ventas
    const clientsWithSales = await db.select({
      id: clients.id,
      name: clients.name,
      email: clients.email,
      subscriptionType: clients.subscriptionType,
      trialStartDate: clients.trialStartDate,
      trialEndDate: clients.trialEndDate,
      isActive: clients.isActive,
      createdAt: clients.createdAt,
      saleDate: resellerSales.saleDate,
      salePrice: resellerSales.salePrice,
      profit: resellerSales.profit,
      notes: resellerSales.notes
    })
    .from(clients)
    .innerJoin(resellerSales, eq(clients.id, resellerSales.clientId))
    .where(eq(resellerSales.resellerId, resellerId))
    .orderBy(desc(clients.createdAt));

    return clientsWithSales;
  }

  async getResellerStats(resellerId: number): Promise<any> {
    const reseller = await this.getResellerById(resellerId);
    if (!reseller) return null;

    const sales = await this.getResellerSalesByReseller(resellerId);

    const currentMonth = new Date();
    currentMonth.setDate(1);
    currentMonth.setHours(0, 0, 0, 0);

    const monthlySales = sales.filter(sale =>
      new Date(sale.saleDate) >= currentMonth
    );

    return {
      totalSales: sales.length,
      monthlyRevenue: monthlySales.reduce((sum, sale) => sum + parseFloat(sale.salePrice), 0),
      monthlyProfit: monthlySales.reduce((sum, sale) => sum + parseFloat(sale.profit), 0),
      monthlyCommission: monthlySales.reduce((sum, sale) => sum + parseFloat(sale.commission), 0),
      accountsQuota: reseller.accountsQuota,
      accountsSold: reseller.accountsSold,
    };
  }

  // Reseller Configuration
  async getResellerConfiguration(resellerId: number): Promise<ResellerConfiguration | undefined> {
    const [result] = await db.select()
      .from(resellerConfiguration)
      .where(eq(resellerConfiguration.resellerId, resellerId));

    if (!result) {
      // Create default configuration
      const defaultConfig = {
        resellerId,
        premiumMonthlyPrice: "$39.99/mes",
        premiumYearlyPrice: "$399.99/año",
        premiumYearlyDiscount: "3 meses gratis",
        defaultTrialDays: 7,
        supportMessage: "Contacta a nuestro equipo para renovar tu suscripción.",
      };

      const [created] = await db.insert(resellerConfiguration)
        .values(defaultConfig)
        .returning();
      return created;
    }

    return result;
  }

  async updateResellerConfiguration(resellerId: number, config: Partial<InsertResellerConfiguration>): Promise<ResellerConfiguration | undefined> {
    const existing = await this.getResellerConfiguration(resellerId);

    if (existing) {
      const [result] = await db.update(resellerConfiguration)
        .set({ ...config, updatedAt: new Date() })
        .where(eq(resellerConfiguration.resellerId, resellerId))
        .returning();
      return result;
    }

    return undefined;
  }

  // Password Reset Tokens
  async createPasswordResetToken(token: InsertPasswordResetToken): Promise<any> {
    const [result] = await db.insert(passwordResetTokens).values(token).returning();
    return result;
  }

  async getPasswordResetToken(token: string): Promise<any> {
    const [result] = await db.select()
      .from(passwordResetTokens)
      .where(eq(passwordResetTokens.token, token));
    return result;
  }

  async markPasswordResetTokenAsUsed(id: number): Promise<boolean> {
    try {
      await db.update(passwordResetTokens)
        .set({ used: true })
        .where(eq(passwordResetTokens.id, id));
      return true;
    } catch (error) {
      console.error('Error marking token as used:', error);
      return false;
    }
  }
}

// Initialize database connection for PRODUCTION VPS PostgreSQL
const connectionString = process.env.DATABASE_URL || "postgresql://stockcel_software:Kc5bpdfkr@localhost:5432/stockcel_software";
if (!connectionString) {
  throw new Error("DATABASE_URL environment variable is required for production");
}

// Production PostgreSQL pool configuration
const pool = new Pool({
  connectionString,
  ssl: false, // VPS PostgreSQL doesn't require SSL
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
});

const db = drizzle(pool);

// Test database connection on startup
pool.connect((err, client, release) => {
  if (err) {
    console.error('❌ Error connecting to PostgreSQL database:', err);
    process.exit(1);
  } else {
    console.log('✅ Connected to production PostgreSQL database');
    release();
  }
});

class DrizzleStorage implements IStorage {
  // Clients
  async createClient(client: InsertClient): Promise<Client> {
    const [result] = await db.insert(clients).values(client).returning();
    return result;
  }

  async getClientById(id: number): Promise<Client | undefined> {
    const [result] = await db.select().from(clients).where(eq(clients.id, id));
    return result;
  }

  async getAllClients(): Promise<Client[]> {
    return await db.select().from(clients);
  }

  async getAllClientsWithAdmins(): Promise<any[]> {
    const allClients = await db.select().from(clients);
    const result = [];

    for (const client of allClients) {
      // Get admin user for this client
      const [adminUser] = await db.select()
        .from(users)
        .where(and(eq(users.clientId, client.id), eq(users.role, 'admin')))
        .limit(1);

      result.push({
        ...client,
        adminUsername: adminUser?.username || 'No admin',
        adminName: adminUser?.username || 'No admin',
        adminEmail: adminUser?.email || '',
      });
    }

    return result;
  }

  async updateClient(id: number, client: Partial<InsertClient>): Promise<Client | undefined> {
    // If email is being updated, check if it already exists in another client
    if (client.email) {
      // Get current client to compare emails
      const currentClient = await db.select().from(clients).where(eq(clients.id, id)).limit(1);

      // Only check for duplicates if the email is actually being changed
      if (currentClient.length > 0 && currentClient[0].email !== client.email) {
        const existingClient = await db.select()
          .from(clients)
          .where(eq(clients.email, client.email))
          .limit(1);

        if (existingClient.length > 0) {
          throw new Error(`Email ${client.email} already exists for another client`);
        }
      }
    }

    const [result] = await db.update(clients).set(client).where(eq(clients.id, id)).returning();
    return result;
  }

  async getClientMovementsSummary(clientId: number): Promise<any> {
    try {
      // Count different types of data for this client
      const [productsCount] = await db.select({ count: sqlOperator`count(*)` }).from(products).where(eq(products.clientId, clientId));
      const [ordersCount] = await db.select({ count: sqlOperator`count(*)` }).from(orders).where(eq(orders.clientId, clientId));
      const [usersCount] = await db.select({ count: sqlOperator`count(*)` }).from(users).where(eq(users.clientId, clientId));
      const [customersCount] = await db.select({ count: sqlOperator`count(*)` }).from(customers).where(eq(customers.clientId, clientId));
      const [cashMovementsCount] = await db.select({ count: sqlOperator`count(*)` }).from(cashMovements).where(eq(cashMovements.clientId, clientId));
      const [paymentsCount] = await db.select({ count: sqlOperator`count(*)` }).from(payments).where(eq(payments.clientId, clientId));
      const [vendorsCount] = await db.select({ count: sqlOperator`count(*)` }).from(vendors).where(eq(vendors.clientId, clientId));

      return {
        products: parseInt(productsCount.count.toString()),
        orders: parseInt(ordersCount.count.toString()),
        users: parseInt(usersCount.count.toString()),
        customers: parseInt(customersCount.count.toString()),
        cashMovements: parseInt(cashMovementsCount.count.toString()),
        payments: parseInt(paymentsCount.count.toString()),
        vendors: parseInt(vendorsCount.count.toString()),
        hasMovements: (
          parseInt(productsCount.count.toString()) > 0 ||
          parseInt(ordersCount.count.toString()) > 0 ||
          parseInt(cashMovementsCount.count.toString()) > 0 ||
          parseInt(paymentsCount.count.toString()) > 0
        )
      };
    } catch (error) {
      console.error('Error getting client movements summary:', error);
      throw error;
    }
  }

  async deleteClient(id: number): Promise<boolean> {
    try {
      // Delete in proper order respecting foreign key constraints

      // 1. First delete records that reference users via user_id
      // Delete cash movements (they have user_id foreign key)
      await db.delete(cashMovements).where(eq(cashMovements.clientId, id));

      // Delete expenses (they have user_id foreign key)
      await db.delete(expenses).where(eq(expenses.clientId, id));

      // Delete debt payments (they have user_id foreign key)
      await db.delete(debtPayments).where(eq(debtPayments.clientId, id));

      // 2. Delete order-related data
      // Get all order IDs for this client first
      const clientOrders = await db.select({ id: orders.id }).from(orders).where(eq(orders.clientId, id));
      const orderIds = clientOrders.map(order => order.id);

      // Delete customer debts (they reference orders)
      await db.delete(customerDebts).where(eq(customerDebts.clientId, id));

      // Delete order items (they reference orders and products)
      if (orderIds.length > 0) {
        await db.delete(orderItems).where(inArray(orderItems.orderId, orderIds));
      }

      // Delete payments (they reference orders)
      await db.delete(payments).where(eq(payments.clientId, id));

      // Delete orders
      await db.delete(orders).where(eq(orders.clientId, id));

      // 3. Delete other client data
      await db.delete(products).where(eq(products.clientId, id));
      await db.delete(cashRegister).where(eq(cashRegister.clientId, id));
      await db.delete(companyConfiguration).where(eq(companyConfiguration.clientId, id));
      await db.delete(vendors).where(eq(vendors.clientId, id));
      await db.delete(customers).where(eq(customers.clientId, id));
      await db.delete(customerDebts).where(eq(customerDebts.clientId, id));
      await db.delete(dailyReports).where(eq(dailyReports.clientId, id));
      await db.delete(generatedReports).where(eq(generatedReports.clientId, id));

      // 4. Finally delete users (no longer referenced by anything)
      await db.delete(users).where(eq(users.clientId, id));

      // 5. Delete the client itself
      const result = await db.delete(clients).where(eq(clients.id, id));
      return result.rowCount > 0;
    } catch (error) {
      console.error('Error deleting client:', error);
      return false;
    }
  }

  // Users - PRODUCTION SECURITY
  async createUser(user: InsertUser): Promise<User> {
    // Hash password with production security if provided
    const userData = { ...user };
    if (userData.password) {
      userData.password = bcrypt.hashSync(userData.password, 12);
    }

    const [result] = await db.insert(users).values(userData).returning();
    console.log(`✅ User created: ${user.email} with role: ${user.role}`);
    return result;
  }

  async getUserById(id: number): Promise<User | undefined> {
    const [result] = await db.select().from(users).where(eq(users.id, id));
    return result;
  }

  async getUserByEmail(email: string): Promise<User | undefined> {
    try {
      console.log(`🔍 Searching for user by email: ${email}`);
      const [result] = await db.select().from(users).where(eq(users.email, email));
      console.log(`📊 getUserByEmail result:`, result ? `Found user ID ${result.id}` : 'No user found');
      return result;
    } catch (error) {
      console.error(`❌ Error in getUserByEmail for ${email}:`, error);
      return undefined;
    }
  }

  async getUserByUsername(username: string): Promise<User | undefined> {
    const [result] = await db.select().from(users).where(eq(users.username, username));
    return result;
  }

  async getUsersByClientId(clientId: number): Promise<User[]> {
    return await db.select().from(users).where(eq(users.clientId, clientId));
  }

  async getUsersByClientIdAndRole(clientId: number, role: string): Promise<User[]> {
    return await db.select().from(users).where(
      and(eq(users.clientId, clientId), eq(users.role, role))
    );
  }

  async updateUser(id: number, user: Partial<InsertUser>, skipPasswordHash: boolean = false): Promise<User | undefined> {
    // Validate user ID
    if (!id || id <= 0) {
      throw new Error('Invalid user ID');
    }

    // Hash password with enhanced security
    const userData = { ...user };
    if (userData.password && !skipPasswordHash) {
      // Validate password strength
      if (userData.password.length < 8) {
        throw new Error('Password must be at least 8 characters long');
      }

      // Only hash if password doesn't start with $2b$ (bcrypt hash format)
      if (!userData.password.startsWith('$2b$')) {
        // Increased salt rounds for better security
        userData.password = bcrypt.hashSync(userData.password, 14);
        console.log(`🔐 Password hashed for user ID: ${id}`);
      } else {
        console.log(`⚠️ Password already hashed for user ID: ${id}, skipping hash`);
      }
    }

    // Validate email format if provided
    if (userData.email) {
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(userData.email)) {
        throw new Error('Invalid email format');
      }
    }

    const [result] = await db.update(users).set(userData).where(eq(users.id, id)).returning();
    console.log(`✅ User updated successfully: ID ${id}`);
    return result;
  }

  async deleteUser(id: number): Promise<boolean> {
    const result = await db.delete(users).where(eq(users.id, id));
    return result.rowCount > 0;
  }

  // Products
  async createProduct(product: InsertProduct): Promise<Product> {
    const [result] = await db.insert(products).values(product).returning();
    return result;
  }

  async getProductById(id: number): Promise<Product | undefined> {
    const [result] = await db.select().from(products).where(eq(products.id, id));
    return result;
  }

  async getProductsByClientId(clientId: number): Promise<Product[]> {
    return await db.select().from(products).where(eq(products.clientId, clientId));
  }

  async getProductByImei(imei: string, clientId: number): Promise<Product | undefined> {
    const [result] = await db.select().from(products).where(
      and(eq(products.imei, imei), eq(products.clientId, clientId))
    );
    return result;
  }

  async updateProduct(id: number, product: Partial<InsertProduct>, userId?: number): Promise<Product | undefined> {
    // Get the existing product first to track changes
    const [existing] = await db.select().from(products).where(eq(products.id, id));
    if (!existing) return undefined;

    // Update the product
    const [result] = await db.update(products)
      .set({ ...product, updatedAt: new Date() })
      .where(eq(products.id, id))
      .returning();

    // Create history record if userId is provided
    if (userId && result) {
      const changes = [];
      const fieldNames = {
        'status': 'Estado',
        'costPrice': 'Precio Costo',
        'quality': 'Calidad',
        'battery': 'Batería',
        'provider': 'Proveedor',
        'observations': 'Observaciones',
        'model': 'Modelo',
        'storage': 'Almacenamiento',
        'color': 'Color',
        'imei': 'IMEI'
      };

      const fieldsToTrack = ['status', 'costPrice', 'quality', 'battery', 'provider', 'observations', 'model', 'storage', 'color', 'imei'];

      for (const field of fieldsToTrack) {
        if (product[field as keyof typeof product] !== undefined && product[field as keyof typeof product] !== existing[field as keyof typeof existing]) {
          const oldValue = existing[field as keyof typeof existing] || 'vacío';
          const newValue = product[field as keyof typeof product] || 'vacío';
          const fieldName = fieldNames[field as keyof typeof fieldNames] || field;
          changes.push(`${fieldName}: ${oldValue} → ${newValue}`);
        }
      }

      if (changes.length > 0) {
        try {
          await this.createProductHistory({
            clientId: existing.clientId,
            productId: id,
            previousStatus: existing.status,
            newStatus: product.status || existing.status,
            userId: userId,
            notes: `Cambios: ${changes.join(', ')}`,
          });
        } catch (error) {
          console.error('Error creating product history:', error);
        }
      }
    }

    return result;
  }

  async updateProductsByImeis(imeis: string[], clientId: number, updates: Partial<InsertProduct>, userId: number): Promise<any> {
    console.log('DrizzleStorage: Updating products by IMEIs:', {imeis, clientId, updates, userId });

    try {
      // Get existing products first to track changes
      const existingProducts = await db.select()
        .from(products)
        .where(
          and(
            inArray(products.imei, imeis),
            eq(products.clientId, clientId)
          )
        );

      // Update products matching the IMEIs
      const result = await db.update(products)
        .set({
          ...updates,
          updatedAt: new Date()
        })
        .where(
          and(
            inArray(products.imei, imeis),
            eq(products.clientId, clientId)
          )
        )
        .returning();

      // Create history records for each updated product
      for (const existingProduct of existingProducts) {
        const changes = [];
        const fieldNames = {
          'status': 'Estado',
          'costPrice': 'Precio Costo',
          'quality': 'Calidad',
          'battery': 'Batería',
          'provider': 'Proveedor',
          'observations': 'Observaciones',
          'model': 'Modelo',
          'storage': 'Almacenamiento',
          'color': 'Color',
          'imei': 'IMEI'
        };

        const fieldsToTrack = ['status', 'costPrice', 'quality', 'battery', 'provider', 'observations', 'model', 'storage', 'color', 'imei'];

        for (const field of fieldsToTrack) {
          if (updates[field as keyof typeof updates] !== undefined && updates[field as keyof typeof updates] !== existingProduct[field as keyof typeof existingProduct]) {
            const oldValue = existingProduct[field as keyof typeof existingProduct] || 'vacío';
            const newValue = updates[field as keyof typeof updates] || 'vacío';
            const fieldName = fieldNames[field as keyof typeof fieldNames] || field;
            changes.push(`${fieldName}: ${oldValue} → ${newValue}`);
          }
        }

        if (changes.length > 0) {
          try {
            await this.createProductHistory({
              clientId: existingProduct.clientId,
              productId: existingProduct.id,
              previousStatus: existingProduct.status,
              newStatus: updates.status || existingProduct.status,
              userId: userId,
              notes: `Actualización masiva - ${changes.join(', ')}`,
            });
          } catch (error) {
            console.error('Error creating product history for IMEI:', existingProduct.imei, error);
          }
        }
      }

      console.log('DrizzleStorage: Batch update completed:', result.length, 'products updated');
      return {
        success: true,
        updatedCount: result.length,
        updatedProducts: result
      };
    } catch (error) {
      console.error('DrizzleStorage: Error in batch update:', error);
      throw error;
    }
  }

  async deleteProduct(id: number): Promise<boolean> {
    const result = await db.delete(products).where(eq(products.id, id));
    return result.rowCount > 0;
  }

  async searchProducts(clientId: number, filters: {
    search?: string;
    status?: string;
    storage?: string;
    model?: string;
    quality?: string;
    battery?: string;
    provider?: string;
    priceMin?: number;
    priceMax?: number;
    limit?: number;
    offset?: number;
  }): Promise<Product[]> {
    // Input validation
    if (!clientId || clientId <= 0) {
      throw new Error('Invalid client ID');
    }

    const conditions = [eq(products.clientId, clientId)];

    // Sanitize and validate search inputs
    if (filters.search) {
      const sanitizedSearch = filters.search.trim().substring(0, 100);
      conditions.push(
        or(
          ilike(products.model, `%${sanitizedSearch}%`),
          ilike(products.imei, `%${sanitizedSearch}%`),
          ilike(products.provider, `%${sanitizedSearch}%`)
        )
      );
    }

    if (filters.status) {
      const validStatuses = ['disponible', 'reservado', 'vendido', 'tecnico_interno', 'tecnico_externo', 'a_reparar', 'extravio'];
      if (validStatuses.includes(filters.status)) {
        conditions.push(eq(products.status, filters.status));
      }
    }

    if (filters.storage) {
      conditions.push(eq(products.storage, filters.storage));
    }

    if (filters.model) {
      const sanitizedModel = filters.model.trim().substring(0, 100);
      conditions.push(ilike(products.model, `%${sanitizedModel}%`));
    }

    if (filters.quality) {
      conditions.push(eq(products.quality, filters.quality));
    }

    if (filters.battery) {
      conditions.push(eq(products.battery, filters.battery));
    }

    if (filters.provider) {
      const sanitizedProvider = filters.provider.trim().substring(0, 100);
      conditions.push(ilike(products.provider, `%${sanitizedProvider}%`));
    }

    if (filters.priceMin !== undefined && filters.priceMin >= 0) {
      conditions.push(gte(products.costPrice, filters.priceMin.toString()));
    }

    if (filters.priceMax !== undefined && filters.priceMax >= 0) {
      conditions.push(lte(products.costPrice, filters.priceMax.toString()));
    }

    let query = db.select().from(products).where(and(...conditions));

    // Add pagination with reasonable defaults
    const limit = Math.min(filters.limit || 100, 1000); // Max 1000 records
    const offset = Math.max(filters.offset || 0, 0);

    query = query.limit(limit).offset(offset);

    return await query.orderBy(desc(products.updatedAt));
  }

  // Payment methods
  async createPayment(payment: InsertPayment): Promise<Payment> {
    console.log('DrizzleStorage: Creating payment with data:', payment);
    const [result] = await db.insert(payments).values(payment).returning();
    console.log('DrizzleStorage: Payment created successfully:', result);

    // CORRECCIÓN CRÍTICA: No crear movimientos automáticos aquí para evitar duplicación
    // Los movimientos de caja se crean desde los endpoints específicos (/api/debt-payments)
    console.log('DrizzleStorage: ✅ Payment created WITHOUT automatic cash movement to prevent duplication');

    return result;
  }

  // Helper function to extract currency from payment method
  private getCurrencyFromPaymentMethod(paymentMethod: string): string {
    // Handle financiera cases specifically
    if (paymentMethod === 'financiera_usd') {
      return 'USD'; // Financiera payment in USD (no conversion needed)
    } else if (paymentMethod === 'financiera_ars') {
      return 'ARS'; // Financiera payment in ARS (needs conversion to USD)
    } else if (paymentMethod.includes('_usd')) {
      return 'USD';
    } else if (paymentMethod.includes('_ars')) {
      return 'ARS';
    } else if (paymentMethod.includes('_usdt')) {
      return 'USDT';
    }
    return 'USD'; // Default
  }

  async getPaymentsByOrderId(orderId: number): Promise<Payment[]> {
    console.log('DrizzleStorage: Getting payments for order ID:', orderId);
    const result = await db.select().from(payments).where(eq(payments.orderId, orderId));
    console.log('DrizzleStorage: Found payments:', result);
    return result;
  }

  async getPaymentsByClientId(clientId: number): Promise<Payment[]> {
    return await db.select().from(payments).where(eq(payments.clientId, clientId));
  }

  // Orders
  async createOrder(order: InsertOrder): Promise<Order> {
    const [result] = await db.insert(orders).values(order).returning();
    return result;
  }

  async getOrderById(id: number): Promise<Order | undefined> {
    const [result] = await db.select().from(orders).where(eq(orders.id, id));
    return result;
  }

  async getOrdersByClientId(clientId: number): Promise<Order[]> {
    try {
      const result = await db.select().from(orders)
        .where(eq(orders.clientId, clientId))
        .orderBy(desc(orders.createdAt));
      return result;
    } catch (error) {
      console.error('Error in getOrdersByClientId:', error);
      return [];
    }
  }

  async updateOrder(id: number, order: Partial<InsertOrder>): Promise<Order | undefined> {
    const [result] = await db.update(orders).set(order).where(eq(orders.id, id)).returning();
    return result;
  }

  async deleteOrder(id: number): Promise<boolean> {
    const result = await db.delete(orders).where(eq(orders.id, id));
    return result.rowCount > 0;
  }

  async getOrdersByClientIdWithVendor(clientId: number): Promise<any[]> {
    try {
      const result = await db
        .select({
          id: orders.id,
          clientId: orders.clientId,
          vendorId: orders.vendorId,
          customerId: orders.customerId,
          orderNumber: orders.orderNumber,
          customerName: orders.customerName,
          customerPhone: orders.customerPhone,
          customerEmail: orders.customerEmail,
          totalUsd: orders.totalUsd,
          status: orders.status,
          paymentStatus: orders.paymentStatus,
          observations: orders.observations,
          notes: orders.notes,
          shippingType: orders.shippingType,
          shippingAddress: orders.shippingAddress,
          createdAt: orders.createdAt,
          updatedAt: orders.updatedAt,
          vendorName: vendors.name
        })
        .from(orders)
        .leftJoin(vendors, eq(orders.vendorId, vendors.id))
        .where(eq(orders.clientId, clientId))
        .orderBy(desc(orders.createdAt));
      return result;
    } catch (error) {
      console.error('Error in getOrdersByClientIdWithVendor:', error);
      return [];
    }
  }

  async getOrdersWithItemsAndProducts(clientId: number, vendorId?: number | null): Promise<any[]> {
    try {
      // Build base query conditions
      const conditions = [eq(orders.clientId, clientId)];
      if (vendorId) {
        conditions.push(eq(orders.vendorId, vendorId));
      }

      // Get orders with complete vendor information including commission
      const ordersResult = await db
        .select({
          id: orders.id,
          clientId: orders.clientId,
          vendorId: orders.vendorId,
          customerId: orders.customerId,
          orderNumber: orders.orderNumber,
          customerName: orders.customerName,
          customerPhone: orders.customerPhone,
          customerEmail: orders.customerEmail,
          totalUsd: orders.totalUsd,
          status: orders.status,
          paymentStatus: orders.paymentStatus,
          observations: orders.observations,
          notes: orders.notes,
          shippingType: orders.shippingType,
          shippingAddress: orders.shippingAddress,
          createdAt: orders.createdAt,
          updatedAt: orders.updatedAt,
          vendorName: vendors.name,
          vendorCommission: vendors.commissionPercentage,
          vendorEmail: vendors.email,
          vendorPhone: vendors.phone,
          vendorIsActive: vendors.isActive
        })
        .from(orders)
        .leftJoin(vendors, eq(orders.vendorId, vendors.id))
        .where(and(...conditions))
        .orderBy(desc(orders.createdAt));

      // For each order, get its items with products
      const result = [];
      for (const order of ordersResult) {
        const orderItemsResult = await db
          .select({
            id: orderItems.id,
            orderId: orderItems.orderId,
            productId: orderItems.productId,
            quantity: orderItems.quantity,
            priceUsd: orderItems.priceUsd,
            paymentMethod: orderItems.paymentMethod,
            exchangeRate: orderItems.exchangeRate,
            amountUsd: orderItems.amountUsd,
            createdAt: orderItems.createdAt,
            // Product fields
            productImei: products.imei,
            productModel: products.model,
            productStorage: products.storage,
            productColor: products.color,
            productCostPrice: products.costPrice,
            productStatus: products.status
          })
          .from(orderItems)
          .leftJoin(products, eq(orderItems.productId, products.id))
          .where(eq(orderItems.orderId, order.id));

        const itemsWithProducts = orderItemsResult.map(item => ({
          id: item.id,
          orderId: item.orderId,
          productId: item.productId,
          quantity: item.quantity,
          priceUsd: item.priceUsd,
          paymentMethod: item.paymentMethod,
          exchangeRate: item.exchangeRate,
          amountUsd: item.amountUsd,
          createdAt: item.createdAt,
          product: item.productImei ? {
            id: item.productId,
            imei: item.productImei,
            model: item.productModel,
            storage: item.productStorage,
            color: item.productColor,
            costPrice: item.productCostPrice,
            status: item.productStatus
          } : null
        }));

        // Create complete vendor object if vendor exists
        const vendor = order.vendorId ? {
          id: order.vendorId,
          name: order.vendorName,
          commissionPercentage: order.vendorCommission,
          email: order.vendorEmail,
          phone: order.vendorPhone,
          isActive: order.vendorIsActive
        } : null;

        result.push({
          ...order,
          vendor: vendor,
          orderItems: itemsWithProducts
        });
      }

      return result;
    } catch (error) {
      console.error('Error in getOrdersWithItemsAndProducts:', error);
      return [];
    }
  }

  async getOrdersByDateRange(clientId: number, startDate: Date, endDate: Date): Promise<Order[]> {
    try {
      const result = await db
        .select()
        .from(orders)
        .where(and(
          eq(orders.clientId, clientId),
          gte(orders.createdAt, startDate),
          lt(orders.createdAt, endDate)
        ))
        .orderBy(desc(orders.createdAt));
      return result;
    } catch (error) {
      console.error('Error in getOrdersByDateRange:', error);
      return [];
    }
  }

  // Order Items
  async createOrderItem(orderItem: InsertOrderItem): Promise<OrderItem> {
    const [result] = await db.insert(orderItems).values(orderItem).returning();
    return result;
  }

  async getOrderItemsByOrderId(orderId: number): Promise<OrderItem[]> {
    return await db.select().from(orderItems).where(eq(orderItems.orderId, orderId));
  }

  async getOrderItemsWithProductsByOrderId(orderId: number): Promise<any[]> {
    console.log('DrizzleStorage: Getting order items with products for order ID:', orderId);

    // Get order items first, then get products
    const orderItemsResult = await db.select().from(orderItems).where(eq(orderItems.orderId, orderId));
    console.log('DrizzleStorage: Found order items:', orderItemsResult);

    const result = [];
    for (const item of orderItemsResult) {
      const productResult = await db.select().from(products).where(eq(products.id, item.productId));
      const product = productResult[0] || null;
      result.push({
        ...item,
        product
      });
    }

    console.log('DrizzleStorage: Final result with products:', result);
    return result;
  }

  async deleteOrderItem(id: number): Promise<boolean> {
    const result = await db.delete(orderItems).where(eq(orderItems.id, id));
    return result.rowCount > 0;
  }

  // Stub implementations for other methods to keep interface compatibility
  async createProductHistory(history: InsertProductHistory): Promise<ProductHistory> {
    const [result] = await db.insert(productHistory).values(history).returning();
    return result;
  }

  async getProductHistoryByProductId(productId: number): Promise<ProductHistory[]> {
    return await db.select().from(productHistory).where(eq(productHistory.productId, productId));
  }

  async getProductHistoryByClientId(clientId: number): Promise<ProductHistory[]> {
    return await db.select().from(productHistory).where(eq(productHistory.clientId, clientId));
  }

  async getProductsWithAlerts(clientId: number): Promise<any[]> {
    return [];
  }

  async createCurrencyExchange(exchange: InsertCurrencyExchange): Promise<CurrencyExchange> {
    const [result] = await db.insert(currencyExchanges).values(exchange).returning();
    return result;
  }

  async getCurrencyExchangesByClientId(clientId: number): Promise<CurrencyExchange[]> {
    return await db.select().from(currencyExchanges).where(eq(currencyExchanges.clientId, clientId));
  }

  async getCurrencyExchangesByDateRange(clientId: number, startDate: Date, endDate: Date): Promise<CurrencyExchange[]> {
    return await db.select().from(currencyExchanges).where(
      and(
        eq(currencyExchanges.clientId, clientId),
        gte(currencyExchanges.createdAt, startDate),
        lt(currencyExchanges.createdAt, endDate)
      )
    );
  }

  async createCashRegister(cashRegisterData: InsertCashRegister): Promise<CashRegister> {
    const [result] = await db.insert(cashRegister).values(cashRegisterData).returning();
    return result;
  }

  async getCurrentCashRegister(clientId: number): Promise<CashRegister | undefined> {
    const [result] = await db.select().from(cashRegister).where(
      and(eq(cashRegister.clientId, clientId), eq(cashRegister.isOpen, true))
    );
    return result;
  }

  async getCashRegisterByDate(clientId: number, dateStr: string): Promise<CashRegister | undefined> {
    const targetDate = new Date(dateStr);
    const startOfDay = new Date(targetDate);
    startOfDay.setHours(0, 0, 0, 0);

    const endOfDay = new Date(targetDate);
    endOfDay.setHours(23, 59, 59, 999);

    const [result] = await db.select().from(cashRegister).where(
      and(
        eq(cashRegister.clientId, clientId),
        gte(cashRegister.date, startOfDay),
        lte(cashRegister.date, endOfDay)
      )
    );
    return result;
  }

  async updateCashRegister(id: number, cashRegisterData: Partial<InsertCashRegister>): Promise<CashRegister | undefined> {
    const [result] = await db.update(cashRegister).set(cashRegisterData).where(eq(cashRegister.id, id)).returning();
    return result;
  }

  async getDailySales(clientId: number, startDate: Date, endDate: Date): Promise<number> {
    const result = await db.select().from(orders).where(
      and(
        eq(orders.clientId, clientId),
        gte(orders.createdAt, startDate),
        lt(orders.createdAt, endDate)
      )
    );

    return result.reduce((sum, order) => sum + parseFloat(order.totalUsd), 0);
  }

  async createConfiguration(config: InsertConfiguration): Promise<Configuration> {
    const [result] = await db.insert(configuration).values(config).returning();
    return result;
  }

  async getConfigurationByKey(clientId: number, key: string): Promise<Configuration | undefined> {
    const [result] = await db.select().from(configuration).where(
      and(eq(configuration.clientId, clientId), eq(configuration.key, key))
    );
    return result;
  }

  async getConfigurationsByClientId(clientId: number): Promise<Configuration[]> {
    return await db.select().from(configuration).where(eq(configuration.clientId, clientId));
  }

  async updateConfiguration(clientId: number, key: string, value: string): Promise<Configuration> {
    const existing = await this.getConfigurationByKey(clientId, key);
    if (existing) {
      const [result] = await db.update(configuration)
        .set({ value })
        .where(and(eq(configuration.clientId, clientId), eq(configuration.key, key)))
        .returning();
      return result;
    } else {
      return await this.createConfiguration({ clientId, key, value });
    }
  }

  async createCompanyConfiguration(config: InsertCompanyConfiguration): Promise<CompanyConfiguration> {
    const [result] = await db.insert(companyConfiguration).values(config).returning();
    return result;
  }

  async getCompanyConfigurationByClientId(clientId: number): Promise<CompanyConfiguration | undefined> {
    const [result] = await db.select().from(companyConfiguration).where(eq(companyConfiguration.clientId, clientId));
    return result;
  }

  async updateCompanyConfiguration(id: number, config: Partial<InsertCompanyConfiguration>): Promise<CompanyConfiguration | undefined> {
    const [result] = await db.update(companyConfiguration).set(config).where(eq(companyConfiguration.id, id)).returning();
    return result;
  }

  async createVendor(vendor: InsertVendor): Promise<Vendor> {
    const [result] = await db.insert(vendors).values(vendor).returning();
    return result;
  }

  async getVendorById(id: number): Promise<Vendor | undefined> {
    const [result] = await db.select().from(vendors).where(eq(vendors.id, id));
    return result;
  }

  async getVendorsByClientId(clientId: number): Promise<Vendor[]> {
    return await db.select().from(vendors).where(eq(vendors.clientId, clientId));
  }

  async updateVendor(id: number, vendor: Partial<InsertVendor>): Promise<Vendor | undefined> {
    const [result] = await db.update(vendors).set(vendor).where(eq(vendors.id, id)).returning();
    return result;
  }

  async deleteVendor(id: number): Promise<boolean> {
    const result = await db.delete(vendors).where(eq(vendors.id, id));
    return result.rowCount > 0;
  }

  async createCustomer(customer: InsertCustomer): Promise<Customer> {
    const [result] = await db.insert(customers).values(customer).returning();
    return result;
  }

  async getCustomerById(id: number): Promise<Customer | undefined> {
    const [result] = await db.select().from(customers).where(eq(customers.id, id));
    return result;
  }

  async getCustomersByClientId(clientId: number): Promise<Customer[]> {
    return await db.select().from(customers).where(eq(customers.clientId, clientId));
  }

  async updateCustomer(id: number, customer: Partial<InsertCustomer>): Promise<Customer | undefined> {
    const [result] = await db.update(customers).set(customer).where(eq(customers.id, id)).returning();
    return result;
  }

  async deleteCustomer(id: number): Promise<boolean> {
    const result = await db.delete(customers).where(eq(customers.id, id));
    return result.rowCount > 0;
  }

  // Stock Control Sessions
  async createStockControlSession(sessionData: InsertStockControlSession): Promise<StockControlSession> {
    console.log('📊 Creating stock control session with data:', JSON.stringify(sessionData, null, 2));

    try {
      const [session] = await db.insert(stockControlSessions).values({
        clientId: sessionData.clientId,
        userId: sessionData.userId,
        date: new Date(sessionData.date),
        startTime: sessionData.startTime || new Date(),
        totalProducts: sessionData.totalProducts || 0,
        scannedProducts: sessionData.scannedProducts || 0,
        missingProducts: sessionData.missingProducts || 0,
        status: sessionData.status || 'active',
        notes: sessionData.notes || null
      }).returning();

      console.log('✅ Stock control session created successfully:', JSON.stringify(session, null, 2));
      return session;
    } catch (error) {
      console.error('❌ Error in createStockControlSession:', error);
      throw error;
    }
  }

  async getStockControlSessionById(id: number): Promise<StockControlSession | undefined> {
    const [session] = await db.select().from(stockControlSessions).where(eq(stockControlSessions.id, id));
    return session;
  }

  async getActiveStockControlSession(clientId: number): Promise<StockControlSession | undefined> {
    const [session] = await db.select()
      .from(stockControlSessions)
      .where(and(
        eq(stockControlSessions.clientId, clientId),
        eq(stockControlSessions.status, 'active')
      ));
    return session;
  }

  async getStockControlSessionsByClientId(clientId: number): Promise<StockControlSession[]> {
    const sessions = await db.select()
      .from(stockControlSessions)
      .where(eq(stockControlSessions.clientId, clientId))
      .orderBy(desc(stockControlSessions.date));
    return sessions;
  }

  async updateStockControlSession(id: number, session: Partial<InsertStockControlSession>): Promise<StockControlSession | undefined> {
    const [updated] = await db.update(stockControlSessions)
      .set(session)
      .where(eq(stockControlSessions.id, id))
      .returning();
    return updated;
  }

  // Stock Control Items
  async createStockControlItem(item: InsertStockControlItem): Promise<StockControlItem> {
    const [created] = await db.insert(stockControlItems).values(item).returning();
    return created;
  }

  async getStockControlItemsBySessionId(sessionId: number): Promise<StockControlItem[]> {
    const items = await db.select()
      .from(stockControlItems)
      .where(eq(stockControlItems.sessionId, sessionId));
    return items;
  }

  async getStockControlItemsWithProductsBySessionId(sessionId: number): Promise<any[]> {
    const items = await db.select({
      id: stockControlItems.id,
      imei: stockControlItems.imei,
      scannedAt: stockControlItems.scannedAt,
      status: stockControlItems.status,
      model: products.model,
      storage: products.storage,
      color: products.color,
    })
    .from(stockControlItems)
    .leftJoin(products, eq(stockControlItems.productId, products.id))
    .where(eq(stockControlItems.sessionId, sessionId));
    return items;
  }

  async updateStockControlItem(id: number, item: Partial<InsertStockControlItem>): Promise<StockControlItem | undefined> {
    const [updated] = await db.update(stockControlItems)
      .set(item)
      .where(eq(stockControlItems.id, id))
      .returning();
    return updated;
  }

  // Stock Control Helper Methods
  async getProductsForStockControl(clientId: number): Promise<Product[]> {
    const productsForControl = await db.select()
      .from(products)
      .where(and(
        eq(products.clientId, clientId),
        eq(products.status, 'disponible')
      ));

    const reservedProducts = await db.select()
      .from(products)
      .where(and(
        eq(products.clientId, clientId),
        eq(products.status, 'reservado')
      ));

    return [...productsForControl, ...reservedProducts];
  }

  async getMissingProductsFromSession(sessionId: number): Promise<Product[]> {
    // Get session info
    const session = await this.getStockControlSessionById(sessionId);
    if (!session) return [];

    // Get all products that should be controlled
    const allProducts = await this.getProductsForStockControl(session.clientId);

    // Get scanned product IDs
    const scannedItems = await this.getStockControlItemsBySessionId(sessionId);
    const scannedProductIds = scannedItems.map(item => item.productId);

    // Return products not scanned
    return allProducts.filter(product => !scannedProductIds.includes(product.id));
  }

  async getExtraviosProducts(clientId: number): Promise<Product[]> {
    const extraviosProducts = await db.select()
      .from(products)
      .where(and(
        eq(products.clientId, clientId),
        eq(products.status, 'extravio')
      ));
    return extraviosProducts;
  }

  // =======================
  // CASH MOVEMENTS - Enhanced System
  // =======================

  async createCashMovement(movement: InsertCashMovement): Promise<CashMovement> {
    const [result] = await db.insert(cashMovements).values(movement).returning();
    return result;
  }

  async getCashMovementsByClientId(clientId: number): Promise<CashMovement[]> {
    return await db
      .select({
        id: cashMovements.id,
        clientId: cashMovements.clientId,
        cashRegisterId: cashMovements.cashRegisterId,
        type: cashMovements.type,
        subtype: cashMovements.subtype,
        amount: cashMovements.amount,
        currency: cashMovements.currency,
        exchangeRate: cashMovements.exchangeRate,
        amountUsd: cashMovements.amountUsd,
        description: cashMovements.description,
        referenceId: cashMovements.referenceId,
        referenceType: cashMovements.referenceType,
        customerId: cashMovements.customerId,
        vendorId: cashMovements.vendorId,
        userId: cashMovements.userId,
        notes: cashMovements.notes,
        createdAt: cashMovements.createdAt,
        userName: users.username,
        customerName: customers.name
      })
      .from(cashMovements)
      .leftJoin(users, eq(cashMovements.userId, users.id))
      .leftJoin(customers, eq(cashMovements.customerId, customers.id))
      .where(eq(cashMovements.clientId, clientId))
      .orderBy(desc(cashMovements.createdAt));
  }

  async getCashMovementsByType(clientId: number, type: string): Promise<CashMovement[]> {
    return await db
      .select({
        id: cashMovements.id,
        clientId: cashMovements.clientId,
        cashRegisterId: cashMovements.cashRegisterId,
        type: cashMovements.type,
        subtype: cashMovements.subtype,
        amount: cashMovements.amount,
        currency: cashMovements.currency,
        exchangeRate: cashMovements.exchangeRate,
        amountUsd: cashMovements.amountUsd,
        description: cashMovements.description,
        referenceId: cashMovements.referenceId,
        referenceType: cashMovements.referenceType,
        customerId: cashMovements.customerId,
        vendorId: cashMovements.vendorId,
        userId: cashMovements.userId,
        notes: cashMovements.notes,
        createdAt: cashMovements.createdAt,
        userName: users.username,
        customerName: customers.name
      })
      .from(cashMovements)
      .leftJoin(users, eq(cashMovements.userId, users.id))
      .leftJoin(customers, eq(cashMovements.customerId, customers.id))
      .where(and(
        eq(cashMovements.clientId, clientId),
        eq(cashMovements.type, type)
      ))
      .orderBy(desc(cashMovements.createdAt));
  }

  async getCashMovementsByDateRange(clientId: number, startDate: Date, endDate: Date): Promise<CashMovement[]> {
    return await db
      .select({
        id: cashMovements.id,
        clientId: cashMovements.clientId,
        cashRegisterId: cashMovements.cashRegisterId,
        type: cashMovements.type,
        subtype: cashMovements.subtype,
        amount: cashMovements.amount,
        currency: cashMovements.currency,
        exchangeRate: cashMovements.exchangeRate,
        amountUsd: cashMovements.amountUsd,
        description: cashMovements.description,
        referenceId: cashMovements.referenceId,
        referenceType: cashMovements.referenceType,
        customerId: cashMovements.customerId,
        vendorId: cashMovements.vendorId,
        userId: cashMovements.userId,
        notes: cashMovements.notes,
        createdAt: cashMovements.createdAt,
        userName: users.username,
        customerName: customers.name
      })
      .from(cashMovements)
      .leftJoin(users, eq(cashMovements.userId, users.id))
      .leftJoin(customers, eq(cashMovements.customerId, customers.id))
      .where(and(
        eq(cashMovements.clientId, clientId),
        gte(cashMovements.createdAt, startDate),
        lt(cashMovements.createdAt, endDate)
      ))
      .orderBy(desc(cashMovements.createdAt));
  }

  async getCashMovementsWithFilters(clientId: number, filters: {
    type?: string;
    dateFrom?: Date;
    dateTo?: Date;
    customer?: string;
    vendor?: string;
    search?: string;
    paymentMethod?: string;
  }): Promise<CashMovement[]> {
    let query = db
      .select({
        id: cashMovements.id,
        clientId: cashMovements.clientId,
        cashRegisterId: cashMovements.cashRegisterId,
        type: cashMovements.type,
        subtype: cashMovements.subtype,
        amount: cashMovements.amount,
        currency: cashMovements.currency,
        exchangeRate: cashMovements.exchangeRate,
        amountUsd: cashMovements.amountUsd,
        description: cashMovements.description,
        referenceId: cashMovements.referenceId,
        referenceType: cashMovements.referenceType,
        customerId: cashMovements.customerId,
        vendorId: cashMovements.vendorId,
        userId: cashMovements.userId,
        notes: cashMovements.notes,
        createdAt: cashMovements.createdAt,
        userName: users.username,
        customerName: customers.name,
        vendorName: vendors.name
      })
      .from(cashMovements)
      .leftJoin(users, eq(cashMovements.userId, users.id))
      .leftJoin(customers, eq(cashMovements.customerId, customers.id))
      .leftJoin(vendors, eq(cashMovements.vendorId, vendors.id));

    const conditions = [eq(cashMovements.clientId, clientId)];

    // Apply filters
    if (filters.type) {
      conditions.push(eq(cashMovements.type, filters.type));
    }

    if (filters.dateFrom && filters.dateTo) {
      conditions.push(gte(cashMovements.createdAt, filters.dateFrom));
      conditions.push(lt(cashMovements.createdAt, filters.dateTo));
    }

    if (filters.customer) {
      conditions.push(ilike(customers.name, `%${filters.customer}%`));
    }

    if (filters.vendor) {
      conditions.push(
        or(
          ilike(vendors.name, `%${filters.vendor}%`),
          ilike(users.username, `%${filters.vendor}%`)
        )
      );
    }

    if (filters.search) {
      conditions.push(
        or(
          ilike(cashMovements.description, `%${filters.search}%`),
          ilike(cashMovements.notes, `%${filters.search}%`),
          ilike(customers.name, `%${filters.search}%`),
          ilike(users.username, `%${filters.search}%`),
          ilike(vendors.name, `%${filters.search}%`)
        )
      );
    }

    if (filters.paymentMethod) {
      conditions.push(eq(cashMovements.subtype, filters.paymentMethod));
    }

    return await query
      .where(and(...conditions))
      .orderBy(desc(cashMovements.createdAt));
  }

  async getAllCashMovementsForExport(clientId: number): Promise<CashMovement[]> {
    return await db
      .select({
        id: cashMovements.id,
        clientId: cashMovements.clientId,
        cashRegisterId: cashMovements.cashRegisterId,
        type: cashMovements.type,
        subtype: cashMovements.subtype,
        amount: cashMovements.amount,
        currency: cashMovements.currency,
        exchangeRate: cashMovements.exchangeRate,
        amountUsd: cashMovements.amountUsd,
        description: cashMovements.description,
        referenceId: cashMovements.referenceId,
        referenceType: cashMovements.referenceType,
        customerId: cashMovements.customerId,
        vendorId: cashMovements.vendorId,
        userId: cashMovements.userId,
        notes: cashMovements.notes,
        createdAt: cashMovements.createdAt,
        userName: users.username,
        customerName: customers.name,
        vendorName: vendors.name
      })
      .from(cashMovements)
      .leftJoin(users, eq(cashMovements.userId, users.id))
      .leftJoin(customers, eq(cashMovements.customerId, customers.id))
      .leftJoin(vendors, eq(cashMovements.vendorId, vendors.id))
      .where(eq(cashMovements.clientId, clientId))
      .orderBy(desc(cashMovements.createdAt));
  }

  // =======================
  // EXPENSES
  // =======================

  async createExpense(expense: InsertExpense): Promise<Expense> {
    const [result] = await db.insert(expenses).values(expense).returning();
    return result;
  }

  async getExpensesByClientId(clientId: number): Promise<Expense[]> {
    return await db
      .select({
        id: expenses.id,
        clientId: expenses.clientId,
        cashRegisterId: expenses.cashRegisterId,
        category: expenses.category,
        description: expenses.description,
        amount: expenses.amount,
        currency: expenses.currency,
        exchangeRate: expenses.exchangeRate,
        amountUsd: expenses.amountUsd,
        paymentMethod: expenses.paymentMethod,
        providerId: expenses.providerId,
        userId: expenses.userId,
        receiptNumber: expenses.receiptNumber,
        notes: expenses.notes,
        expenseDate: expenses.expenseDate,
        createdAt: expenses.createdAt,
        userName: users.username
      })
      .from(expenses)
      .leftJoin(users, eq(expenses.userId, users.id))
      .where(eq(expenses.clientId, clientId))
      .orderBy(desc(expenses.createdAt));
  }

  async getExpensesByCategory(clientId: number, category: string): Promise<Expense[]> {
    return await db
      .select({
        id: expenses.id,
        clientId: expenses.clientId,
        cashRegisterId: expenses.cashRegisterId,
        category: expenses.category,
        description: expenses.description,
        amount: expenses.amount,
        currency: expenses.currency,
        exchangeRate: expenses.exchangeRate,
        amountUsd: expenses.amountUsd,
        paymentMethod: expenses.paymentMethod,
        providerId: expenses.providerId,
        userId: expenses.userId,
        receiptNumber: expenses.receiptNumber,
        notes: expenses.notes,
        expenseDate: expenses.expenseDate,
        createdAt: expenses.createdAt,
        userName: users.username
      })
      .from(expenses)
      .leftJoin(users, eq(expenses.userId, users.id))
      .where(and(
        eq(expenses.clientId, clientId),
        eq(expenses.category, category)
      ))
      .orderBy(desc(expenses.createdAt));
  }

  async getExpensesByDateRange(clientId: number, startDate: Date, endDate: Date): Promise<Expense[]> {
    return await db
      .select({
        id: expenses.id,
        clientId: expenses.clientId,
        cashRegisterId: expenses.cashRegisterId,
        category: expenses.category,
        description: expenses.description,
        amount: expenses.amount,
        currency: expenses.currency,
        exchangeRate: expenses.exchangeRate,
        amountUsd: expenses.amountUsd,
        paymentMethod: expenses.paymentMethod,
        providerId: expenses.providerId,
        userId: expenses.userId,
        receiptNumber: expenses.receiptNumber,
        notes: expenses.notes,
        expenseDate: expenses.expenseDate,
        createdAt: expenses.createdAt,
        userName: users.username
      })
      .from(expenses)
      .leftJoin(users, eq(expenses.userId, users.id))
      .where(and(
        eq(expenses.clientId, clientId),
        gte(expenses.expenseDate, startDate),
        lt(expenses.expenseDate, endDate)
      ))
      .orderBy(desc(expenses.createdAt));
  }

  async updateExpense(id: number, updates: Partial<InsertExpense>): Promise<Expense | undefined> {
    const [result] = await db
      .update(expenses)
      .set(updates)
      .where(eq(expenses.id, id))
      .returning();
    return result;
  }

  async deleteExpense(id: number): Promise<boolean> {
    const result = await db.delete(expenses).where(eq(expenses.id, id));
    return result.rowCount > 0;
  }

  // =======================
  // CUSTOMER DEBTS
  // =======================

  async createCustomerDebt(debt: InsertCustomerDebt): Promise<CustomerDebt> {
    const [result] = await db.insert(customerDebts).values(debt).returning();
    return result;
  }

  async getCustomerDebtsByClientId(clientId: number): Promise<CustomerDebt[]> {
    return await db
      .select({
        id: customerDebts.id,
        clientId: customerDebts.clientId,
        customerId: customerDebts.customerId,
        orderId: customerDebts.orderId,
        debtAmount: customerDebts.debtAmount,
        paidAmount: customerDebts.paidAmount,
        remainingAmount: customerDebts.remainingAmount,
        currency: customerDebts.currency,
        status: customerDebts.status,
        dueDate: customerDebts.dueDate,
        paymentHistory: customerDebts.paymentHistory,
        notes: customerDebts.notes,
        createdAt: customerDebts.createdAt,
        updatedAt: customerDebts.updatedAt,
        customerName: customers.name
      })
      .from(customerDebts)
      .leftJoin(customers, eq(customerDebts.customerId, customers.id))
      .where(eq(customerDebts.clientId, clientId))
      .orderBy(desc(customerDebts.createdAt));
  }

  async getCustomerDebtsByCustomerId(customerId: number): Promise<CustomerDebt[]> {
    return await db
      .select({
        id: customerDebts.id,
        clientId: customerDebts.clientId,
        customerId: customerDebts.customerId,
        orderId: customerDebts.orderId,
        debtAmount: customerDebts.debtAmount,
        paidAmount: customerDebts.paidAmount,
        remainingAmount: customerDebts.remainingAmount,
        currency: customerDebts.currency,
        status: customerDebts.status,
        dueDate: customerDebts.dueDate,
        paymentHistory: customerDebts.paymentHistory,
        notes: customerDebts.notes,
        createdAt: customerDebts.createdAt,
        updatedAt: customerDebts.updatedAt,
        customerName: customers.name
      })
      .from(customerDebts)
      .leftJoin(customers, eq(customerDebts.customerId, customers.id))
      .where(eq(customerDebts.customerId, customerId))
      .orderBy(desc(customerDebts.createdAt));
  }

  async getActiveDebts(clientId: number): Promise<CustomerDebt[]> {
    return await db
      .select({
        id: customerDebts.id,
        clientId: customerDebts.clientId,
        customerId: customerDebts.customerId,
        orderId: customerDebts.orderId,
        debtAmount: customerDebts.debtAmount,
        paidAmount: customerDebts.paidAmount,
        remainingAmount: customerDebts.remainingAmount,
        currency: customerDebts.currency,
        status: customerDebts.status,
        dueDate: customerDebts.dueDate,
        paymentHistory: customerDebts.paymentHistory,
        notes: customerDebts.notes,
        createdAt: customerDebts.createdAt,
        updatedAt: customerDebts.updatedAt,
        customerName: customers.name
      })
      .from(customerDebts)
      .leftJoin(customers, eq(customerDebts.customerId, customers.id))
      .where(and(
        eq(customerDebts.clientId, clientId),
        eq(customerDebts.status, 'vigente')
      ))
      .orderBy(desc(customerDebts.createdAt));
  }

  async getActiveDebtByOrderId(orderId: number): Promise<CustomerDebt | undefined> {
    const [debt] = await db.select()
      .from(customerDebts)
      .where(and(
        eq(customerDebts.orderId, orderId),
        eq(customerDebts.status, 'vigente')
      ))
      .limit(1);
    return debt;
  }

  async updateCustomerDebt(id: number, updates: Partial<InsertCustomerDebt>): Promise<CustomerDebt | undefined> {
    const [result] = await db
      .update(customerDebts)
      .set(updates)
      .where(eq(customerDebts.id, id))
      .returning();
    return result;
  }

  // =======================
  // DEBT PAYMENTS
  // =======================

  async createDebtPayment(payment: InsertDebtPayment): Promise<DebtPayment> {
    const [result] = await db.insert(debtPayments).values(payment).returning();
    return result;
  }

  async getDebtPaymentsByDebtId(debtId: number): Promise<DebtPayment[]> {
    return await db
      .select({
        id: debtPayments.id,
        clientId: debtPayments.clientId,
        debtId: debtPayments.debtId,
        cashRegisterId: debtPayments.cashRegisterId,
        amount: debtPayments.amount,
        currency: debtPayments.currency,
        exchangeRate: debtPayments.exchangeRate,
        amountUsd: debtPayments.amountUsd,
        paymentMethod: debtPayments.paymentMethod,
        userId: debtPayments.userId,
        notes: debtPayments.notes,
        paymentDate: debtPayments.paymentDate,
        createdAt: debtPayments.createdAt,
        userName: users.username
      })
      .from(debtPayments)
      .leftJoin(users, eq(debtPayments.userId, users.id))
      .where(eq(debtPayments.debtId, debtId))
      .orderBy(desc(debtPayments.createdAt));
  }

  async getDebtPaymentsByClientId(clientId: number): Promise<DebtPayment[]> {
    return await db
      .select({
        id: debtPayments.id,
        clientId: debtPayments.clientId,
        debtId: debtPayments.debtId,
        cashRegisterId: debtPayments.cashRegisterId,
        amount: debtPayments.amount,
        currency: debtPayments.currency,
        exchangeRate: debtPayments.exchangeRate,
        amountUsd: debtPayments.amountUsd,
        paymentMethod: debtPayments.paymentMethod,
        userId: debtPayments.userId,
        notes: debtPayments.notes,
        paymentDate: debtPayments.paymentDate,
        createdAt: debtPayments.createdAt,
        userName: users.username
      })
      .from(debtPayments)
      .leftJoin(users, eq(debtPayments.userId, users.id))
      .where(eq(debtPayments.clientId, clientId))
      .orderBy(desc(debtPayments.createdAt));
  }

  // =======================
  // DAILY REPORTS
  // =======================

  async createDailyReport(report: InsertDailyReport): Promise<DailyReport> {
    const [result] = await db.insert(dailyReports).values(report).returning();
    return result;
  }

  async getDailyReportsByClientId(clientId: number): Promise<DailyReport[]> {
    return await db
      .select()
      .from(dailyReports)
      .where(eq(dailyReports.clientId, clientId))
      .orderBy(desc(dailyReports.reportDate));
  }

  async getDailyReportByDate(clientId: number, date: Date): Promise<DailyReport | undefined> {
    const [result] = await db
      .select()
      .from(dailyReports)
      .where(and(
        eq(dailyReports.clientId, clientId),
        eq(dailyReports.reportDate, date)
      ));
    return result;
  }

  async generateAutoDailyReport(clientId: number, date: Date): Promise<DailyReport> {
    // Get all movements for the day
    const startDate = new Date(date);
    startDate.setHours(0, 0, 0, 0);
    const endDate = new Date(date);
    endDate.setHours(23, 59, 59, 999);

    // Calculate totals using raw SQL for better performance
    const totalsQuery = await db.execute(sqlOperator`
      SELECT
        COALESCE(SUM(CASE WHEN cm.type IN ('ingreso', 'venta') THEN CAST(cm.amount_usd AS DECIMAL) ELSE 0 END), 0) as total_income,
        COALESCE(SUM(CASE WHEN e.id IS NOT NULL THEN CAST(e.amount_usd AS DECIMAL) ELSE 0 END), 0) as total_expenses,
        COALESCE(SUM(CASE WHEN dp.id IS NOT NULL THEN CAST(dp.amount_usd AS DECIMAL) ELSE 0 END), 0) as total_debt_payments,
        COUNT(cm.id) as total_movements
      FROM cash_movements cm
      LEFT JOIN expenses e ON e.client_id = ${clientId} AND e.expense_date >= ${startDate} AND e.expense_date < ${endDate}
      LEFT JOIN debt_payments dp ON dp.client_id = ${clientId} AND dp.payment_date >= ${startDate} AND dp.payment_date < ${endDate}
      WHERE cm.client_id = ${clientId}
        AND cm.created_at >= ${startDate}
        AND cm.created_at < ${endDate}
    `);

    const totals = totalsQuery.rows[0] as any;
    const totalIncome = parseFloat(totals.total_income || "0");
    const totalExpenses = parseFloat(totals.total_expenses || "0");
    const totalDebtPayments = parseFloat(totals.total_debt_payments || "0");
    const netProfit = totalIncome - totalExpenses;

    const reportData = {
      summary: {
        totalIncome,
        totalExpenses,
        totalDebtPayments,
        netProfit,
        movementsCount: parseInt(totals.total_movements || "0")
      },
      timestamp: new Date().toISOString()
    };

    const report = await this.createDailyReport({
      clientId,
      reportDate: date,
      openingBalance: "1000.00", // Should get from previous day's closing
      totalIncome: totalIncome.toFixed(2),
      totalExpenses: totalExpenses.toFixed(2),
      totalDebts: "0.00", // Calculate active debts
      totalDebtPayments: totalDebtPayments.toFixed(2),
      netProfit: netProfit.toFixed(2),
      vendorCommissions: "0.00", // Calculate vendor commissions
      exchangeRateUsed: "1200.00", // Get current rate
      closingBalance: (1000 + netProfit).toFixed(2),
      totalMovements: parseInt(totals.total_movements || "0"),
      reportData: JSON.stringify(reportData),
      isAutoGenerated: true
    });

    return report;
  }

  // =======================
  // REAL-TIME STATE
  // =======================

  async getRealTimeCashState(clientId: number): Promise<any> {
    try {
      // Validate client ID
      if (!clientId || clientId <= 0) {
        throw new Error('Invalid client ID');
      }

      console.log('🔄 getRealTimeCashState: Starting optimized calculation for clientId:', clientId);

      // Use aggregated queries for better performance - FIXED ITERATION ERROR
      const incomeQuery = await db.execute(sqlOperator`
        SELECT
          COALESCE(SUM(CASE WHEN type IN ('venta', 'ingreso', 'pago_deuda') THEN CAST(amount_usd AS DECIMAL) ELSE 0 END), 0) as total_income,
          COALESCE(SUM(CASE WHEN type IN ('gasto', 'egreso') THEN CAST(amount_usd AS DECIMAL) ELSE 0 END), 0) as total_expenses,
          COALESCE(SUM(CASE WHEN subtype = 'efectivo_ars' AND type IN ('venta', 'ingreso', 'pago_deuda') THEN CAST(amount AS DECIMAL) ELSE 0 END), 0) as efectivo_ars,
          COALESCE(SUM(CASE WHEN subtype = 'efectivo_usd' AND type IN ('venta', 'ingreso', 'pago_deuda') THEN CAST(amount_usd AS DECIMAL) ELSE 0 END), 0) as efectivo_usd,
          COALESCE(SUM(CASE WHEN subtype = 'transferencia_ars' AND type IN ('venta', 'ingreso', 'pago_deuda') THEN CAST(amount AS DECIMAL) ELSE 0 END), 0) as transferencia_ars,
          COALESCE(SUM(CASE WHEN subtype = 'transferencia_usd' AND type IN ('venta', 'ingreso', 'pago_deuda') THEN CAST(amount_usd AS DECIMAL) ELSE 0 END), 0) as transferencia_usd,
          COALESCE(SUM(CASE WHEN subtype = 'transferencia_usdt' AND type IN ('venta', 'ingreso', 'pago_deuda') THEN CAST(amount_usd AS DECIMAL) ELSE 0 END), 0) as transferencia_usdt,
          COALESCE(SUM(CASE WHEN subtype = 'financiera_ars' AND type IN ('venta', 'ingreso', 'pago_deuda') THEN CAST(amount AS DECIMAL) ELSE 0 END), 0) as financiera_ars,
          COALESCE(SUM(CASE WHEN subtype = 'financiera_usd' AND type IN ('venta', 'ingreso', 'pago_deuda') THEN CAST(amount_usd AS DECIMAL) ELSE 0 END), 0) as financiera_usd,
          COUNT(*) as total_movements
        FROM cash_movements
        WHERE client_id = ${clientId}
      `);

      // CORRECCIÓN CRÍTICA: Acceder correctamente al primer resultado
      const aggregatedData = incomeQuery.rows[0] as any;
      const totalIncome = parseFloat(aggregatedData.total_income || "0");
      const totalExpenses = parseFloat(aggregatedData.total_expenses || "0");

      let salesByPaymentMethod = {
        total_ventas: totalIncome,
        total_gastos: totalExpenses,
        efectivo_ars: parseFloat(aggregatedData.efectivo_ars || "0"),
        efectivo_usd: parseFloat(aggregatedData.efectivo_usd || "0"),
        transferencia_ars: parseFloat(aggregatedData.transferencia_ars || "0"),
        transferencia_usd: parseFloat(aggregatedData.transferencia_usd || "0"),
        transferencia_usdt: parseFloat(aggregatedData.transferencia_usdt || "0"),
        financiera_ars: parseFloat(aggregatedData.financiera_ars || "0"),
        financiera_usd: parseFloat(aggregatedData.financiera_usd || "0"),
        balance_final: 0,
        deudas_pendientes: 0
      };

      // Get opening balance from cash register
      const currentCashRegister = await this.getCurrentCashRegister(clientId);
      const openingBalance = parseFloat(currentCashRegister?.initialUsd || "1000");

      // Calculate active debts
      let totalActiveDebts = 0;
      try {
        totalActiveDebts = await this.getTotalDebtsAmount(clientId);
        console.log('✅ Active debts calculated:', totalActiveDebts);
      } catch (error) {
        console.error('❌ Error calculating debts:', error);
        totalActiveDebts = 0;
      }

      salesByPaymentMethod.deudas_pendientes = totalActiveDebts;
      salesByPaymentMethod.balance_final = openingBalance + salesByPaymentMethod.total_ventas - salesByPaymentMethod.total_gastos;

      console.log('🎯 Final Results:', salesByPaymentMethod);

      return {
        totalBalanceUsd: salesByPaymentMethod.balance_final.toFixed(2),
        dailySalesUsd: salesByPaymentMethod.total_ventas.toFixed(2),
        dailyExpensesUsd: salesByPaymentMethod.total_gastos.toFixed(2),
        totalActiveDebtsUsd: totalActiveDebts.toFixed(2),
        openingBalance: openingBalance.toFixed(2),
        salesByPaymentMethod,
        lastUpdated: new Date().toISOString()
      };
    } catch (error) {
      console.error('❌ Error in getRealTimeCashState:', error);
      // Return safe defaults
      return {
        totalBalanceUsd: "1000.00",
        dailySalesUsd: "0.00",
        dailyExpensesUsd: "0.00",
        totalActiveDebtsUsd: "0.00",
        openingBalance: "1000.00",
        salesByPaymentMethod: {
          total_ventas: 0,
          total_gastos: 0,
          efectivo_ars: 0,
          efectivo_usd: 0,
          transferencia_ars: 0,
          transferencia_usd: 0,
          transferencia_usdt: 0,
          financiera_ars: 0,
          financiera_usd: 0,
          balance_final: 1000,
          deudas_pendientes: 0
        },
        lastUpdated: new Date().toISOString()
      };
    }
  }

  async getTotalDebtsAmount(clientId: number): Promise<number> {
    console.log('🔍 Calculating total debts for clientId:', clientId);

    // Get real debts from the database
    const activeDebts = await this.getActiveDebts(clientId);
    const totalDebts = activeDebts.reduce((sum, debt) => {
      return sum + parseFloat(debt.remainingAmount || '0');
    }, 0);

    console.log('📊 Total active debts calculated:', totalDebts);
    return totalDebts;
  }

  async getTotalPendingVendorPayments(clientId: number): Promise<number> {
    // Implementation for vendor payments if needed
    return 0;
  }

  async getStockValue(clientId: number): Promise<{usd: number, ars: number}> {
    const result = await db.execute(sqlOperator`
      SELECT COALESCE(SUM(CAST(cost_price AS DECIMAL)), 0) as total
      FROM products
      WHERE client_id = ${clientId} AND status IN ('disponible', 'reservado')
    `);

    const totalUsd = parseFloat(result.rows[0]?.total || "0");
    const exchangeRate = 1200; // Should get from current rate

    return {
      usd: totalUsd,
      ars: totalUsd * exchangeRate
    };
  }

  async getOrCreateTodayCashRegister(clientId: number): Promise<CashRegister> {
    try {
      const today = new Date();
      today.setHours(0, 0, 0, 0);

      // Try to get current cash register
      let cashRegister = await this.getCurrentCashRegister(clientId);

      // If there's an open cash register from a previous day, close it first
      if (cashRegister && !this.isSameDay(new Date(cashRegister.date), today)) {
        console.log('🔄 Auto-closing previous day cash register before opening new one');
        await this.autoCloseCashRegister(clientId);
        cashRegister = null; // Force creation of new register
      }

      // If no cash register for today, create one automatically
      if (!cashRegister) {
        // Get previous day's closing balance to use as opening balance
        const previousClosingBalance = await this.getPreviousDayClosingBalance(clientId);
        const initialUsd = previousClosingBalance.toString();
        const initialArs = "0.00";
        const initialUsdt = "0.00";

        console.log(`💰 Creating new cash register with opening balance: $${initialUsd}`);

        cashRegister = await this.createCashRegister({
          clientId,
          date: today,
          initialUsd,
          initialArs,
          initialUsdt,
          currentUsd: initialUsd,
          currentArs: initialArs,
          currentUsdt: initialUsdt,
          dailySales: "0.00",
          totalDebts: "0.00",
          totalExpenses: "0.00",
          dailyGlobalExchangeRate: "1200.00",
          isOpen: true,
          isActive: true
        });
      }

      return cashRegister;
    } catch (error) {
      console.error('Error in getOrCreateTodayCashRegister:', error);
      throw new Error('No se pudo obtener o crear la caja del día');
    }
  }

  async autoCloseCashRegister(clientId: number): Promise<CashRegister | undefined> {
    const currentRegister = await this.getCurrentCashRegister(clientId);
    if (!currentRegister) return undefined;

    // Calculate closing values
    const realTimeState = await this.getRealTimeCashState(clientId);

    // Create daily report before closing
    await this.createAutoDailyReport(clientId, currentRegister, realTimeState);

    const updatedRegister = await this.updateCashRegister(currentRegister.id, {
      isOpen: false,
      closedAt: new Date(),
      autoClosedAt: new Date(),
      currentUsd: realTimeState.totalBalanceUsd,
      dailySales: realTimeState.dailySalesUsd,
      totalExpenses: realTimeState.dailyExpensesUsd,
      totalDebts: realTimeState.totalActiveDebtsUsd
    });

    return updatedRegister;
  }

  async createAutoDailyReport(clientId: number, cashRegister: any, realTimeState: any): Promise<void> {
    try {
      // Get current time in Argentina timezone (UTC-3)
      const now = new Date();
      const argentinaOffset = -3 * 60; // UTC-3 in minutes
      const argentinaTime = new Date(now.getTime() + (argentinaOffset - now.getTimezoneOffset()) * 60 * 1000);

      // Get all movements from today
      const todayStart = new Date(argentinaTime.getFullYear(), argentinaTime.getMonth(), argentinaTime.getDate());
      const todayEnd = new Date(todayStart.getTime() + 24 * 60 * 60 * 1000 - 1);

      const movements = await db.select()
        .from(cashMovements)
        .where(and(
          eq(cashMovements.clientId, clientId),
          gte(cashMovements.createdAt, todayStart),
          lt(cashMovements.createdAt, todayEnd)
        ));

      // Calculate totals
      let totalIncome = 0;
      let totalExpenses = 0;
      let totalDebtPayments = 0;

      movements.forEach(movement => {
        const amountUsd = parseFloat(movement.amountUsd);
        if (movement.type === 'venta' || movement.type === 'ingreso') {
          totalIncome += amountUsd;
        } else if (movement.type === 'gasto') {
          totalExpenses += Math.abs(amountUsd);
        } else if (movement.type === 'pago_deuda') {
          totalDebtPayments += amountUsd;
        }
      });

      const netProfit = totalIncome - totalExpenses;
      const openingBalance = parseFloat(cashRegister.initialUsd || "0");
      const closingBalance = parseFloat(realTimeState.totalBalanceUsd);

      // Create daily report
      const [dailyReport] = await db.insert(dailyReports).values({
        clientId,
        reportDate: argentinaTime,
        openingBalance: openingBalance.toString(),
        totalIncome: totalIncome.toString(),
        totalExpenses: totalExpenses.toString(),
        totalDebts: realTimeState.totalActiveDebtsUsd,
        totalDebtPayments: totalDebtPayments.toString(),
        netProfit: netProfit.toString(),
        vendorCommissions: "0", // TODO: Calculate vendor commissions
        exchangeRateUsed: "1200", // TODO: Get from configuration
        closingBalance: closingBalance.toString(),
        totalMovements: movements.length,
        reportData: JSON.stringify({
          movements: movements.slice(0, 100), // Store first 100 movements
          summary: {
            totalIncome,
            totalExpenses,
            totalDebtPayments,
            netProfit,
            openingBalance,
            closingBalance
          }
        }),
        isAutoGenerated: true
      }).returning();

      // Generate automatic Excel report
      if (dailyReport) {
        await this.generateAutoExcelReport(clientId, dailyReport.id, argentinaTime);
      }

      console.log(`📊 Daily report created for client ${clientId} - ${argentinaTime.toISOString().split('T')[0]}`);
      console.log(`📊 Excel report generated automatically for ${argentinaTime.toISOString().split('T')[0]}`);
    } catch (error) {
      console.error('❌ Error creating daily report:', error);
      throw error;
    }
  }

  // Helper method to check if two dates are the same day
  private isSameDay(date1: Date, date2: Date): boolean {
    return date1.getFullYear() === date2.getFullYear() &&
           date1.getMonth() === date2.getMonth() &&
           date1.getDate() === date2.getDate();
  }

  // Get previous day's closing balance to use as opening balance for new day
  async getPreviousDayClosingBalance(clientId: number): Promise<number> {
    try {
      // Try to get the most recent daily report
      const [lastReport] = await db.select()
        .from(dailyReports)
        .where(eq(dailyReports.clientId, clientId))
        .orderBy(desc(dailyReports.reportDate))
        .limit(1);

      if (lastReport && lastReport.closingBalance) {
        const balance = parseFloat(lastReport.closingBalance);
        console.log(`📊 Using previous day closing balance: $${balance}`);
        return balance;
      }

      // If no previous report, try to get the last closed cash register
      const [lastClosedRegister] = await db.select()
        .from(cashRegister)
        .where(and(
          eq(cashRegister.clientId, clientId),
          eq(cashRegister.isOpen, false)
        ))
        .orderBy(desc(cashRegister.closedAt))
        .limit(1);

      if (lastClosedRegister && lastClosedRegister.currentUsd) {
        const balance = parseFloat(lastClosedRegister.currentUsd);
        console.log(`📊 Using last closed register balance: $${balance}`);
        return balance;
      }

      // Default opening balance
      console.log(`📊 Using default opening balance: $1000.00`);
      return 1000.00;
    } catch (error) {
      console.error('❌ Error getting previous day closing balance:', error);
      return 1000.00; // Safe default
    }
  }

  async getDailyReports(clientId: number, limit: number = 30): Promise<any[]> {
    return await db.select()
      .from(dailyReports)
      .where(eq(dailyReports.clientId, clientId))
      .orderBy(desc(dailyReports.reportDate))
      .limit(limit);
  }

  // =======================
  // GENERATED REPORTS
  // =======================

  async createGeneratedReport(report: InsertGeneratedReport): Promise<GeneratedReport> {
    const [result] = await db.insert(generatedReports).values(report).returning();
    return result;
  }

  async getGeneratedReportsByClientId(clientId: number): Promise<GeneratedReport[]> {
    return await db
      .select()
      .from(generatedReports)
      .leftJoin(dailyReports, eq(generatedReports.dailyReportId, dailyReports.id))
      .where(eq(generatedReports.clientId, clientId))
      .orderBy(desc(generatedReports.generatedAt));
  }

  async getGeneratedReportById(id: number): Promise<GeneratedReport | undefined> {
    const [result] = await db
      .select()
      .from(generatedReports)
      .where(eq(generatedReports.id, id));
    return result;
  }

  async generateAutoExcelReport(clientId: number, dailyReportId: number, reportDate: Date): Promise<void> {
    try {
      // Get comprehensive data for the report
      const [
        cashMovements,
        expenses,
        debtPayments,
        vendorPerformance,
        paymentMethods // This seems to be missing in the query, assuming it's for payment method counts
      ] = await Promise.all([
        this.getAllCashMovementsForExport(clientId),
        this.getExpensesByClientId(clientId),
        this.getDebtPaymentsByClientId(clientId),
        this.getVendorPerformanceRanking(clientId),
        this.getPaymentMethodsSummary(clientId) // Assuming this method exists or needs to be created
      ]);

      // Filter data for the specific date
      const dateStr = reportDate.toISOString().split('T')[0];
      const filteredMovements = cashMovements.filter((mov: any) =>
        mov.createdAt.toISOString().split('T')[0] === dateStr
      );
      const filteredExpenses = expenses.filter((exp: any) =>
        exp.expenseDate.toISOString().split('T')[0] === dateStr
      );
      const filteredDebtPayments = debtPayments.filter((debt: any) =>
        debt.paymentDate.toISOString().split('T')[0] === dateStr
      );

      // Calculate payment method breakdown
      const paymentMethodBreakdown: any = {};
      filteredMovements.forEach((movement: any) => {
        const method = movement.subtype || 'No especificado';
        if (!paymentMethodBreakdown[method]) {
          paymentMethodBreakdown[method] = { count: 0, totalUsd: 0 };
        }
        paymentMethodBreakdown[method].count++;
        paymentMethodBreakdown[method].totalUsd += parseFloat(movement.amountUsd || 0);
      });

      // Prepare comprehensive report content
      const reportContent = {
        date: dateStr,
        paymentMethodBreakdown,
        detailedPayments: filteredMovements,
        detailedExpenses: filteredExpenses,
        cashMovements: filteredMovements,
        vendorPerformance: vendorPerformance,
        summary: {
          totalIncome: filteredMovements
            .filter((m: any) => m.type === 'venta' || m.type === 'ingreso')
            .reduce((sum: number, m: any) => sum + parseFloat(m.amountUsd || 0), 0),
          totalExpenses: filteredExpenses
            .reduce((sum: number, e: any) => sum + parseFloat(e.amountUsd || 0), 0),
          totalDebtPayments: filteredDebtPayments
            .reduce((sum: number, d: any) => sum + parseFloat(d.amountUsd || 0), 0),
          totalMovements: filteredMovements.length
        }
      };

      // Generate Excel content as CSV format
      const excelContent = this.generateExcelCsvContent(reportContent);
      const fileName = `StockCel_Reporte_Diario_${dateStr.replace(/-/g, '_')}.csv`;

      // Store the generated report
      await this.createGeneratedReport({
        clientId,
        dailyReportId,
        reportDate,
        reportType: 'excel',
        fileName,
        fileData: Buffer.from(excelContent).toString('base64'),
        fileSize: Buffer.from(excelContent).length,
        reportContent: JSON.stringify(reportContent),
        isAutoGenerated: true
      });

      console.log(`📊 Excel report generated automatically: ${fileName}`);
    } catch (error) {
      console.error('Error generating auto Excel report:', error);
    }
  }

  // Placeholder for getPaymentMethodsSummary, replace with actual implementation if needed
  private async getPaymentMethodsSummary(clientId: number): Promise<any> {
    console.warn("getPaymentMethodsSummary is not implemented, returning empty object.");
    return {};
  }

  private generateExcelCsvContent(reportContent: any): string {
    const lines = [];

    // Header
    lines.push('STOCKCEL - REPORTE DIARIO AUTOMATICO');
    lines.push(`Fecha: ${reportContent.date}`);
    lines.push('');

    // Summary section
    lines.push('RESUMEN EJECUTIVO');
    lines.push('Concepto,Valor USD');
    lines.push(`Ingresos Totales,${reportContent.summary.totalIncome.toFixed(2)}`);
    lines.push(`Gastos Totales,${reportContent.summary.totalExpenses.toFixed(2)}`);
    lines.push(`Pagos de Deudas,${reportContent.summary.totalDebtPayments.toFixed(2)}`);
    lines.push(`Ganancia Neta,${(reportContent.summary.totalIncome - reportContent.summary.totalExpenses).toFixed(2)}`);
    lines.push(`Total Movimientos,${reportContent.summary.totalMovements}`);
    lines.push('');

    // Payment methods breakdown
    lines.push('DESGLOSE POR METODOS DE PAGO');
    lines.push('Metodo,Cantidad,Total USD');
    Object.entries(reportContent.paymentMethodBreakdown).forEach(([method, data]: any) => {
      lines.push(`${method},${data.count},${data.totalUsd.toFixed(2)}`);
    });
    lines.push('');

    // Detailed movements
    lines.push('MOVIMIENTOS DE CAJA DETALLADOS');
    lines.push('Fecha,Tipo,Descripcion,Metodo,Moneda,Monto Original,Monto USD,Cliente,Vendedor');
    reportContent.detailedPayments.forEach((movement: any) => {
      lines.push([
        new Date(movement.createdAt).toLocaleDateString('es-ES'),
        movement.type,
        movement.description || '-',
        movement.subtype || '-',
        movement.currency,
        movement.amount,
        movement.amountUsd,
        movement.customerName || '-',
        movement.vendorName || '-'
      ].join(','));
    });
    lines.push('');

    // Vendor performance
    lines.push('RENDIMIENTO DE VENDEDORES');
    lines.push('Vendedor,Ventas,Ingresos USD,Ganancia,Comision');
    reportContent.vendorPerformance.forEach((vendor: any) => {
      lines.push([
        vendor.vendorName,
        vendor.totalSales || 0,
        vendor.totalRevenue || 0,
        vendor.totalProfit || 0,
        vendor.commission || 0
      ].join(','));
    });

    return lines.join('\n');
  }

  async getAllCashMovementsForCompleteExport(clientId: number): Promise<any[]> {
    // Get all movements with related data for comprehensive export
    const movements = await db.select({
      id: cashMovements.id,
      type: cashMovements.type,
      subtype: cashMovements.subtype,
      amount: cashMovements.amount,
      currency: cashMovements.currency,
      exchangeRate: cashMovements.exchangeRate,
      amountUsd: cashMovements.amountUsd,
      description: cashMovements.description,
      customerName: customers.name,
      vendorName: vendors.name,
      userId: cashMovements.userId,
      referenceId: cashMovements.referenceId,
      referenceType: cashMovements.referenceType,
      createdAt: cashMovements.createdAt,
      userName: users.username
    })
    .from(cashMovements)
    .leftJoin(users, eq(cashMovements.userId, users.id))
    .leftJoin(customers, eq(cashMovements.customerId, customers.id))
    .leftJoin(vendors, eq(cashMovements.vendorId, vendors.id))
    .where(eq(cashMovements.clientId, clientId))
    .orderBy(desc(cashMovements.createdAt));

    return movements;
  }

  // =======================
  // AUTO-SYNC MONITOR METHODS
  // =======================

  async getOrdersByDate(clientId: number, date: string): Promise<Order[]> {
    const startDate = new Date(date + 'T00:00:00.000Z');
    const endDate = new Date(date + 'T23:59:59.999Z');

    return await db.select()
      .from(orders)
      .where(and(
        eq(orders.clientId, clientId),
        gte(orders.createdAt, startDate),
        lte(orders.createdAt, endDate)
      ))
      .orderBy(desc(orders.createdAt));
  }

  async getCashMovementsByOrderId(orderId: number): Promise<CashMovement[]> {
    return await db.select()
      .from(cashMovements)
      .where(eq(cashMovements.referenceId, orderId));
  }

  // =======================
  // AUTOMATIC CASH SCHEDULING
  // =======================

  async scheduleCashOperations(clientId: number): Promise<{
    status: 'open' | 'closed' | 'no_config' | 'error';
    message?: string;
    nextOpen: Date | null;
    nextClose: Date | null;
    currentTime: string;
    actualCashRegister?: any;
    config?: any;
  }> {
    try {
      console.log(`🔍 [DEBUG] scheduleCashOperations called for clientId: ${clientId}`);

      const config = await cashScheduleStorage.getScheduleConfig(clientId);
      if (!config) {
        return {
          status: 'no_config',
          message: 'No hay configuración de horarios automáticos',
          nextOpen: null,
          nextClose: null,
          currentTime: new Date().toISOString()
        };
      }

      // CORRECCIÓN CRÍTICA: Verificar estado REAL de la caja en la base de datos
      const currentCashRegister = await this.getCurrentCashRegister(clientId);
      const today = new Date().toISOString().split('T')[0];

      // Get current Argentina time
      const now = new Date();
      const argentinaTime = new Date(now.toLocaleString("en-US", { timeZone: "America/Argentina/Buenos_Aires" }));

      console.log(`🕐 Current Argentina time: ${argentinaTime.toLocaleString('es-AR')}`);

      // Calculate next operations
      const currentHour = argentinaTime.getHours();
      const currentMinute = argentinaTime.getMinutes();
      const currentTimeInMinutes = currentHour * 60 + currentMinute;

      const openTimeInMinutes = (config.openHour || 0) * 60 + (config.openMinute || 0);
      const closeTimeInMinutes = (config.closeHour || 23) * 60 + (config.closeMinute || 59);

      // CORRECCIÓN: Determinar estado basado en CAJA REAL + HORARIOS
      let status = 'closed';

      if (currentCashRegister && currentCashRegister.isOpen) {
        // Si hay una caja abierta, verificar que sea de hoy
        const registerDate = new Date(currentCashRegister.date).toISOString().split('T')[0];
        if (registerDate === today) {
          status = 'open';
          console.log(`✅ [SCHEDULE] Cash register is OPEN for today ${today} - ID: ${currentCashRegister.id}`);
        } else {
          status = 'closed';
          console.log(`⚠️ [SCHEDULE] Cash register is from different date ${registerDate}, today is ${today}`);
        }
      } else {
        status = 'closed';
        console.log(`❌ [SCHEDULE] No active cash register found for client ${clientId}`);
      }

      // Calculate next open/close times
      const todayDate = new Date(argentinaTime);
      const tomorrow = new Date(argentinaTime);
      tomorrow.setDate(tomorrow.getDate() + 1);

      let nextOpen = null;
      let nextClose = null;

      if (config.autoOpenEnabled) {
        if (status === 'closed' && currentTimeInMinutes < openTimeInMinutes) {
          // Next open is today
          nextOpen = new Date(todayDate);
          nextOpen.setHours(config.openHour || 0, config.openMinute || 0, 0, 0);
        } else {
          // Next open is tomorrow
          nextOpen = new Date(tomorrow);
          nextOpen.setHours(config.openHour || 0, config.openMinute || 0, 0, 0);
        }
      }

      if (config.autoCloseEnabled) {
        if (status === 'open' && currentTimeInMinutes < closeTimeInMinutes) {
          // Next close is today
          nextClose = new Date(todayDate);
          nextClose.setHours(config.closeHour || 23, config.closeMinute || 59, 0, 0);
        } else {
          // Next close is tomorrow
          nextClose = new Date(tomorrow);
          nextClose.setHours(config.closeHour || 23, config.closeMinute || 59, 0, 0);
        }
      }

      const result = {
        status,
        nextOpen: nextOpen?.toISOString() ?? null,
        nextClose: nextClose?.toISOString() ?? null,
        currentTime: argentinaTime.toISOString(),
        actualCashRegister: currentCashRegister ? {
          id: currentCashRegister.id,
          isOpen: currentCashRegister.isOpen,
          date: currentCashRegister.date
        } : null,
        config: {
          openHour: config.openHour,
          openMinute: config.openMinute,
          closeHour: config.closeHour,
          closeMinute: config.closeMinute,
          autoOpenEnabled: config.autoOpenEnabled,
          autoCloseEnabled: config.autoCloseEnabled
        }
      };

      console.log(`📅 Schedule for client ${clientId}: Status=${status}, Cash Register ID=${currentCashRegister?.id}, isOpen=${currentCashRegister?.isOpen}, Current ${argentinaTime.toLocaleDateString()}, ${argentinaTime.toLocaleTimeString()}, Next Open: ${nextOpen?.toLocaleDateString()}, ${nextOpen?.toLocaleTimeString()}, Next Close: ${nextClose?.toLocaleDateString()}, ${nextClose?.toLocaleTimeString()}`);

      return result;
    } catch (error) {
      console.error('Error getting cash schedule:', error);
      return {
        status: 'error',
        message: 'Error al obtener programación de caja',
        nextOpen: null,
        nextClose: null,
        currentTime: new Date().toISOString()
      };
    }
  }

  async checkAndProcessAutomaticOperations(clientId: number): Promise<{
    closed?: CashRegister;
    opened?: CashRegister;
    notification?: string;
  }> {
    const now = new Date();
    const currentRegister = await this.getCurrentCashRegister(clientId);
    const schedule = await this.scheduleCashOperations(clientId);

    let result: any = {};

    // Check if it's time to close (23:59:00)
    const closeTime = new Date();
    closeTime.setHours(23, 59, 0, 0);

    if (now >= closeTime && currentRegister && currentRegister.isOpen) {
      result.closed = await this.autoCloseCashRegister(clientId);
      result.notification = `🕐 Caja cerrada automáticamente a las ${closeTime.toLocaleTimeString()}. Reabrirá mañana a las 00:00:00`;
    }

    // Check if it's time to open (00:00:00)
    const openTime = new Date();
    openTime.setHours(0, 0, 0, 0);

    if (now >= openTime && (!currentRegister || !currentRegister.isOpen)) {
      // Get previous day's closing balance for opening balance
      const previousBalance = result.closed?.currentUsd || "0.00";

      result.opened = await this.createCashRegister({
        clientId,
        date: new Date(),
        initialUsd: previousBalance,
        initialArs: "0.00",
        initialUsdt: "0.00",
        currentUsd: previousBalance,
        currentArs: "0.00",
        currentUsdt: "0.00",
        dailySales: "0.00",
        totalDebts: "0.00",
        totalExpenses: "0.00",
        dailyGlobalExchangeRate: "1200.00",
        isOpen: true,
        isActive: true
      });

      result.notification = `🌅 Caja abierta automáticamente a las ${openTime.toLocaleTimeString()}. Cerrará hoy a las 23:59:00`;
    }

    return result;
  }

  // Resellers
  async createReseller(reseller: InsertReseller): Promise<Reseller> {
    console.log('🔐 Creando revendedor - Email:', reseller.email);
    console.log('🔐 Password original length:', reseller.password.length);
    console.log('🔐 Password original (primeros 10):', reseller.password.substring(0, 10));

    const hashedPassword = bcrypt.hashSync(reseller.password, 10);
    console.log('🔐 Password hasheado length:', hashedPassword.length);
    console.log('🔐 Password hasheado (primeros 20):', hashedPassword.substring(0, 20));

    const [result] = await db.insert(resellers).values({
      ...reseller,
      password: hashedPassword,
    }).returning();

    console.log('✅ Revendedor creado exitosamente - ID:', result.id);
    return result;
  }

  async getResellers(): Promise<Reseller[]> {
    return await db.select().from(resellers).orderBy(desc(resellers.createdAt));
  }

  async getResellerById(id: number): Promise<Reseller | undefined> {
    const [result] = await db.select().from(resellers).where(eq(resellers.id, id));
    return result;
  }

  async getResellerByEmail(email: string): Promise<Reseller | undefined> {
    const [result] = await db.select().from(resellers).where(eq(resellers.email, email));
    return result;
  }

  async updateReseller(id: number, reseller: Partial<InsertReseller>): Promise<Reseller | undefined> {
    const updateData: any = { ...reseller, updatedAt: new Date() };

    // Hash password if provided
    if (reseller.password) {
      updateData.password = bcrypt.hashSync(reseller.password, 10);
      console.log('🔐 Actualizando revendedor - Password hasheado correctamente');
    }

    const [result] = await db.update(resellers)
      .set(updateData)
      .where(eq(resellers.id, id))
      .returning();
    return result;
  }

  async deleteReseller(id: number): Promise<boolean> {
    // Delete related data first
    await db.delete(resellerSales).where(eq(resellerSales.resellerId, id));
    await db.delete(resellerConfiguration).where(eq(resellerConfiguration.resellerId, id));

    // Delete reseller
    const result = await db.delete(resellers).where(eq(resellers.id, id));
    return result.rowCount > 0;
  }

  // Reseller Sales
  async createResellerSale(resellerId: number, saleData: any): Promise<ResellerSale> {
    // Create client first
    const newClient = await this.createClient({
      name: saleData.clientName,
      email: saleData.clientEmail,
      subscriptionType: saleData.subscriptionType,
      trialStartDate: new Date(),
      trialEndDate: new Date(Date.now() + (saleData.trialDays * 24 * 60 * 60 * 1000)),
    });

    // Calculate profit (salePrice - costPerAccount)
    const profit = saleData.salePrice - saleData.costPerAccount;

    // Create sale record
    const [result] = await db.insert(resellerSales).values({
      resellerId,
      clientId: newClient.id,
      costPerAccount: saleData.costPerAccount.toString(),
      salePrice: saleData.salePrice.toString(),
      profit: profit.toString(),
      subscriptionType: saleData.subscriptionType,
      trialDays: saleData.trialDays,
      notes: saleData.notes,
    }).returning();

    // Update reseller stats - get current values first
    const currentReseller = await db.select().from(resellers).where(eq(resellers.id, resellerId)).limit(1);
    if (currentReseller.length > 0) {
      await db.update(resellers)
        .set({
          accountsSold: currentReseller[0].accountsSold + 1,
          totalEarnings: currentReseller[0].totalEarnings + profit,
          totalPaid: currentReseller[0].totalPaid + saleData.costPerAccount,
          updatedAt: new Date(),
        })
        .where(eq(resellers.id, resellerId));
    }

    return result;
  }

  async getResellerSales(): Promise<ResellerSale[]> {
    const salesWithClients = await db.select({
      id: resellerSales.id,
      resellerId: resellerSales.resellerId,
      clientId: resellerSales.clientId,
      costPerAccount: resellerSales.costPerAccount,
      salePrice: resellerSales.salePrice,
      profit: resellerSales.profit,
      subscriptionType: resellerSales.subscriptionType,
      trialDays: resellerSales.trialDays,
      saleDate: resellerSales.saleDate,
      notes: resellerSales.notes,
      createdAt: resellerSales.createdAt,
      clientName: clients.name,
      clientEmail: clients.email,
    })
    .from(resellerSales)
    .leftJoin(clients, eq(resellerSales.clientId, clients.id))
    .orderBy(desc(resellerSales.saleDate));

    return salesWithClients as any[];
  }

  async getResellerSalesByReseller(resellerId: number): Promise<ResellerSale[]> {
    console.log(`🔍 getResellerSalesByReseller called for resellerId: ${resellerId}`);

    const salesWithClients = await db.select({
      id: resellerSales.id,
      resellerId: resellerSales.resellerId,
      clientId: resellerSales.clientId,
      costPerAccount: resellerSales.costPerAccount,
      salePrice: resellerSales.salePrice,
      profit: resellerSales.profit,
      subscriptionType: resellerSales.subscriptionType,
      trialDays: resellerSales.trialDays,
      saleDate: resellerSales.saleDate,
      notes: resellerSales.notes,
      createdAt: resellerSales.createdAt,
      clientName: clients.name,
      clientEmail: clients.email,
    })
    .from(resellerSales)
    .leftJoin(clients, eq(resellerSales.clientId, clients.id))
    .where(eq(resellerSales.resellerId, resellerId))
    .orderBy(desc(resellerSales.saleDate));

    console.log(`📊 getResellerSalesByReseller result: Found ${salesWithClients.length} sales`);
    console.log(`📊 Sales IDs: ${salesWithClients.map(s => s.id).join(', ')}`);

    return salesWithClients as any[];
  }

  async getClientsByResellerId(resellerId: number): Promise<any[]> {
    // Obtener clientes creados por este revendedor a través de las ventas
    const clientsWithSales = await db.select({
      id: clients.id,
      name: clients.name,
      email: clients.email,
      subscriptionType: clients.subscriptionType,
      trialStartDate: clients.trialStartDate,
      trialEndDate: clients.trialEndDate,
      isActive: clients.isActive,
      createdAt: clients.createdAt,
      saleDate: resellerSales.saleDate,
      salePrice: resellerSales.salePrice,
      profit: resellerSales.profit,
      notes: resellerSales.notes
    })
    .from(clients)
    .innerJoin(resellerSales, eq(clients.id, resellerSales.clientId))
    .where(eq(resellerSales.resellerId, resellerId))
    .orderBy(desc(clients.createdAt));

    return clientsWithSales;
  }

  async getResellerStats(resellerId: number): Promise<any> {
    const reseller = await this.getResellerById(resellerId);
    if (!reseller) return null;

    const sales = await this.getResellerSalesByReseller(resellerId);

    const currentMonth = new Date();
    currentMonth.setDate(1);
    currentMonth.setHours(0, 0, 0, 0);

    const monthlySales = sales.filter(sale =>
      new Date(sale.saleDate) >= currentMonth
    );

    return {
      totalSales: sales.length,
      monthlyRevenue: monthlySales.reduce((sum, sale) => sum + parseFloat(sale.salePrice), 0),
      monthlyProfit: monthlySales.reduce((sum, sale) => sum + parseFloat(sale.profit), 0),
      monthlyCommission: monthlySales.reduce((sum, sale) => sum + parseFloat(sale.commission), 0),
      accountsQuota: reseller.accountsQuota,
      accountsSold: reseller.accountsSold,
    };
  }

  // Reseller Configuration
  async getResellerConfiguration(resellerId: number): Promise<ResellerConfiguration | undefined> {
    const [result] = await db.select()
      .from(resellerConfiguration)
      .where(eq(resellerConfiguration.resellerId, resellerId));

    if (!result) {
      // Create default configuration
      const defaultConfig = {
        resellerId,
        premiumMonthlyPrice: "$39.99/mes",
        premiumYearlyPrice: "$399.99/año",
        premiumYearlyDiscount: "3 meses gratis",
        defaultTrialDays: 7,
        supportMessage: "Contacta a nuestro equipo para renovar tu suscripción.",
      };

      const [created] = await db.insert(resellerConfiguration)
        .values(defaultConfig)
        .returning();
      return created;
    }

    return result;
  }

  async updateResellerConfiguration(resellerId: number, config: Partial<InsertResellerConfiguration>): Promise<ResellerConfiguration | undefined> {
    const existing = await this.getResellerConfiguration(resellerId);

    if (existing) {
      const [result] = await db.update(resellerConfiguration)
        .set({ ...config, updatedAt: new Date() })
        .where(eq(resellerConfiguration.resellerId, resellerId))
        .returning();
      return result;
    }

    return undefined;
  }

  // Password Reset Tokens
  async createPasswordResetToken(token: InsertPasswordResetToken): Promise<any> {
    const [result] = await db.insert(passwordResetTokens).values(token).returning();
    return result;
  }

  async getPasswordResetToken(token: string): Promise<any> {
    const [result] = await db.select()
      .from(passwordResetTokens)
      .where(eq(passwordResetTokens.token, token));
    return result;
  }

  async markPasswordResetTokenAsUsed(id: number): Promise<boolean> {
    try {
      await db.update(passwordResetTokens)
        .set({ used: true })
        .where(eq(passwordResetTokens.id, id));
      return true;
    } catch (error) {
      console.error('Error marking token as used:', error);
      return false;
    }
  }
}

export const storage = new DrizzleStorage();
export { db };